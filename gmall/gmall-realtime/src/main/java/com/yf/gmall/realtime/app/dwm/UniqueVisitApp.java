package com.yf.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yf.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author by yangfan
 * @date 2021/5/9.
 * @desc uv处理逻辑
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        // 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        // 读取kafka配置信息
        String sourceTopic = "dwd_page_log";
        String groupId = "unique_vist_app_group";

        // sink dwm层的kafka主题名称
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> sourceDs = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));
        SingleOutputStreamOperator<JSONObject> jsonDs = sourceDs.map(jsonObj -> JSON.parseObject(jsonObj));

        // 对相同的key（设备id）进行分组
        KeyedStream<JSONObject, String> keyByWithMidDstream = jsonDs.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        // 过滤重复访问
        SingleOutputStreamOperator<JSONObject> filterDs = keyByWithMidDstream.filter(new RichFilterFunction<JSONObject>() {

            private SimpleDateFormat simpleDateFormat;
            private ValueState<String> lastVistDateSate;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 进行初始化操作

                // 初始化日期
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

                // 初始化状态

                // 状态描述器
                ValueStateDescriptor<String> lastViewDateStateDescriptor = new ValueStateDescriptor<>("lastViewDateState", String.class);

                // 设置状态失效时间，过了当天失效，使用构造者设计模式进行创建对象
                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();

                lastViewDateStateDescriptor.enableTimeToLive(stateTtlConfig);
                lastVistDateSate = getRuntimeContext().getState(lastViewDateStateDescriptor);

            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                // 获取状态值
                String lastVistDate = lastVistDateSate.value();
                // 获取埋点中的上一页面的来源
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                // 获取访问时间戳
                String tsDate = simpleDateFormat.format(new Date(jsonObject.getLong("ts")));
                // 如果上一页来源不为空，则则重复访问用户，进行过滤
                if (lastPageId != null && lastPageId.length() != 0) {
                    System.out.println(jsonObject);
                    System.out.println("已访问:lastPageId:" + lastPageId);

                    return false;
                }
                // 有状态值-重复访问；上次访问日志为当天-重复访问；都需要过滤
                if (lastVistDate != null && lastVistDate.length() != 0 && tsDate.equals(lastVistDate)) {
                    System.out.println("已访问:lastVisitDate:" + lastVistDate + ",||tsDate:" + tsDate);
                    return false;
                }else {
                    System.out.println("未访问:lastVisitDate:" + lastVistDate + ",||tsDate:" + tsDate);
                    lastVistDateSate.update(tsDate);
                    return true;
                }

            }
        });

        // json转为string，方便写入kafka
        SingleOutputStreamOperator<String> kafkaDs = filterDs.map(jsonObj -> jsonObj.toString());

        // 将uv的dwm数据发送到kafka
        kafkaDs.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();

    }
}
