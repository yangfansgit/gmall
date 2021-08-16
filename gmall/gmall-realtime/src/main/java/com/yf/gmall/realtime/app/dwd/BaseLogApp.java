package com.yf.gmall.realtime.app.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import com.yf.gmall.realtime.utils.MyKafkaUtil;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author by yangfan
 * @date 2021/4/10.
 * @desc 从 Kafka 中读取 ods 层用户行为日志数据
 */
public class BaseLogApp {
    private final static String TOPIC_START = "dwd_start_log";
    private final static String TOPIC_SHOW = "dwd_show_log";
    private final static String TOPIC_PAGE = "dwd_page_log";

    public static void main(String[] args) throws Exception {
        // 创建flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度，与kafka分区数一致
        env.setParallelism(4);

        /* 生产环境会用到，项目不需要
        // 设置检查点，用于故障恢复
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9000/gmall/flink/checkpoint"));*/


        //从kafka读取数据
        // 查看kafka主题：bin/kafka-topics.sh --bootstrap-server hadoop102:9092 --list
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource("ods_base_log", "ods_dwd_base_log_app");

        DataStream<String> kafkaDs = env.addSource(kafkaSource);


        DataStream<JSONObject> jsonObjectDS = kafkaDs.map(new MapFunction<String, JSONObject>() {

            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return jsonObject;
            }
        });
/*        // lambda表达式写法
        DataStream<JSONObject> jsonObjectDS = kafkaDs.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            return jsonObject;
        });*/

        // 判断新老客
        // JSONObject 输入类型  String key的类型
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjectDS.keyBy(data -> {
            String key = data.getJSONObject("common").getString("mid");
            return key;
        });

        // RichMapFunction才能获取对应的状态
        SingleOutputStreamOperator<JSONObject> jsonDSWithFlag = midKeyedDS.map(new RichMapFunction<JSONObject, JSONObject>() {
            // 简单键控状态，存单一的值
            private ValueState<String> firstVistDateSate;
            // 日期格式化 日期类型 1608263642000
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 在open初始化firstVistDateSate
                firstVistDateSate = getRuntimeContext().getState(
                        new ValueStateDescriptor("newDateMidState", String.class)
                );
                // 初始化日期格式类型
                simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                // 获取当前新老客状态
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                // 获取当前系统时间
                String tsDate = simpleDateFormat.format(new Date(jsonObject.getLong("ts")));
                // 新客可能是假新客，因此需要判断，还有一部分日志不是common日志，对于这种直接忽略
                if ("1".equals(isNew)) {
                    // 获取当前状态对象
                    String newMidDate = firstVistDateSate.value();
                    if (newMidDate != null && newMidDate.length() != 0) {
                        // 状态日期与时间戳日期相比较，相等为新客，不相等不老客
                        if (!newMidDate.equals(tsDate)) {
                            isNew = "0";
                            jsonObject.getJSONObject("common").put("is_new", isNew);
                        }
                    } else {
                        // 就是一个新客,需要保存第一次访问时间
                        firstVistDateSate.update(tsDate);
                    }

                }
                return jsonObject;
            }
        });
        // 使用测输出流进行分流
        // 启动日志
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        // 曝光日志
        OutputTag<String> showTag = new OutputTag<String>("showTag") {
        };
        // 输出到kafka，因此输出是String
        SingleOutputStreamOperator<String> pageDs = jsonDSWithFlag.process(new ProcessFunction<JSONObject, String>() {

            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> out) throws Exception {
                // 启动日志
                JSONObject startJsonObj = jsonObject.getJSONObject("start");
                String dataStr = jsonObject.toString();

                // 判断是否启动日志
                if (startJsonObj != null && startJsonObj.size() > 0) {
                    // 启动日志输出到start测输出流
                    context.output(startTag, dataStr);
                } else {
                    // 不是启动光日志，则是页面日志，即使是曝光日志，也是基于页面日志的，因此需要将曝光日志也算进去
                    out.collect(dataStr);
                    // 曝光日志
                    JSONArray displaysArray = jsonObject.getJSONArray("displays");
                    // 如果是曝光日志，将每一个曝光输出到show测输出流
                    if (displaysArray != null && displaysArray.size() > 0) {
                        for (int i = 0; i < displaysArray.size(); i++) {
                            // 得到每一个曝光模块
                            JSONObject displaysJonObj = displaysArray.getJSONObject(i);
                            // 添加上页面信息,page_id
                            displaysJonObj.put("page_id", jsonObject.getJSONObject("page").getString("page_id"));
                            context.output(showTag, displaysJonObj.toString());
                        }
                    }
                }

            }
        });
        DataStream<String> startDs = pageDs.getSideOutput(startTag);
        DataStream<String> showDs = pageDs.getSideOutput(showTag);


        // sink操作，将处理后的dwd层发送至kafka
        FlinkKafkaProducer<String> startSink = MyKafkaUtil.getKafkaSink(TOPIC_START);
        startDs.addSink(startSink);

        FlinkKafkaProducer<String> showSink = MyKafkaUtil.getKafkaSink(TOPIC_SHOW);
        showDs.addSink(showSink);

        FlinkKafkaProducer<String> pageSink = MyKafkaUtil.getKafkaSink(TOPIC_PAGE);
        pageDs.addSink(pageSink);


        env.execute("dwd");
    }
}
