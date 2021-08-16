package com.yf.gmall.realtime.app.dwd;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author by yangfan
 * @date 2021/4/10.
 * @desc
 */
public class ForTest {
    public void rec(JSONObject json){
        System.out.println(json.toString());
        changeNunm(json);
        System.out.println(json);
    }

    public void changeNunm(JSONObject json) {
        Set<Map.Entry<String, Object>> entries = json.entrySet();

        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
            break;
        }
        System.out.println(entries);
    }

    public static void main(String[] args) throws  Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 7777);
        DataStream<JSONObject> dataStream = inputStream.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                return jsonObject;
            }
        });

        SingleOutputStreamOperator<JSONObject> jsonObjWithEtDs = dataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                })
        );

        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = jsonObjWithEtDs.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("page").getString("page_id");
            }
        });

        WindowedStream<JSONObject, String, TimeWindow> window = jsonObjectStringKeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        window.reduce(new ReduceFunction<JSONObject>() {
            @Override
            public JSONObject reduce(JSONObject jsonObject, JSONObject t1) throws Exception {
                return null;
            }
        });
    }
}
