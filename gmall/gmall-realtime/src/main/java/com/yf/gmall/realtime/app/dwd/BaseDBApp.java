package com.yf.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yf.gmall.realtime.app.func.DimSink;
import com.yf.gmall.realtime.app.func.TableProcessFunction;
import com.yf.gmall.realtime.bean.TableProcess;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import com.yf.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author by yangfan
 * @date 2021/4/17.
 * @desc 处理业务数据
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(4);
        // 设置检查点，精准一次
        // env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        // 检查点一分钟超时时间
        // env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 设置状态后端，将检查点存到hdfs
        // env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9000/gmall/finlk/checkpoint"));

        // kafka读取消息
        // 查看主题 bin/kafka-topics.sh –bootsrap-server hadoop102:9092 --list

        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);

        //读取数据源
        DataStreamSource<String> jsonStrDs = env.addSource(kafkaSource);

        // 转化Json
        SingleOutputStreamOperator<JSONObject> jsonObjDs = jsonStrDs.map(line ->JSON.parseObject(line));

        // 对业务数据进行ETL
        DataStream<JSONObject> filterDs = jsonObjDs.filter(line -> {
            // 长度小于3、没有表名为脏数据，其中data为maxwell导入历史数据的标记，为空说明是对历史数据进行处理
            boolean flag = line.toString().length() >= 3 &&
                    line.getJSONObject("data") != null &&
                    line.getString("table") != null;

            return flag;

        });

        // 对业务数据分流操作
        // 定义侧输出刘标签
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {};
        // 主流，事实数据，写入kafka
        SingleOutputStreamOperator<JSONObject> kafkaDStream = filterDs.process(new TableProcessFunction(hbaseTag));

        DataStream<JSONObject> hbaseDStream = kafkaDStream.getSideOutput(hbaseTag);

        // 维表输出至hbase
        hbaseDStream.addSink(new DimSink());

        // 事实表输出至kafka
        FlinkKafkaProducer<JSONObject> kafkaSinkBySchema = MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("启动 Kafka Sink");
            }

            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                String topic = jsonObject.getString("sink_table");
                JSONObject dataJsonObj = jsonObject.getJSONObject("data");
                return new ProducerRecord(topic, dataJsonObj.toJSONString().getBytes());
            }
        });

        kafkaDStream.addSink(kafkaSinkBySchema);
        kafkaDStream.print("kafka>>>>>>");
        hbaseDStream.print("hbase>>>>>>");

        env.execute();


    }
}
