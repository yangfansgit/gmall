package com.yf.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import com.yf.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;


/**
 * @author by yangfan
 * @date 2021/5/19.
 * @desc
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";

        // 获取页面曝光的kafka流
        DataStreamSource<String> sourceDs = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));

        // 转为JSONobject
        DataStream<JSONObject> jsonObjeDs = sourceDs.map(jsonObj -> JSONObject.parseObject(jsonObj));


        // 设置事件时间戳，由于flink1.12默认使用的是事件事件，所以不需要额外定义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 设置延迟时间和时间戳
        SingleOutputStreamOperator<JSONObject> jsonObjWithEtDs = jsonObjeDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                })
        );

        // 对mid进行分组
        KeyedStream<JSONObject, String> keyByJsonObjDs = jsonObjWithEtDs.keyBy(keyJsonObj -> keyJsonObj.getJSONObject("common").getString("mid"));
        // 设置cep

        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(
                // 条件一，判断访客来源是非站内访问，对于站内跳转的访问不在页面的跳出范围，即来源页为空保留，否则过滤
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            return true;
                        }
                        return false;
                    }
                }
        ).next("next").where(
                // 条件二，在 10 秒时间范围内必须有第二个页面
                // 之前一直想不通这里，既然判断跳出，页面为空应该返回true，这样才能只留下跳出的，这个思路是错误的，下方注释会有解释
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        if (pageId != null && pageId.length() > 0) {
                            return true;
                        }
                        return false;
                    }

                }
        ).within(Time.seconds(10));// 此处限制10秒

        /*以上代码总结：跳出需要满足以下条件：
        * 1.来源页为空，为空说明是从外站进行访问的
        * 2.10秒内没有再次的访问
        * pageId不可能为空，因此需要返回true，10秒内有访问不算跳出，10秒后即使有跳转了，但还是算跳出
        * 在等待该用户下次访问时，需要限制一个时间，超过时间会出发,否则一直处于等待状状态
        * */

        // 根据表达式筛选流
        PatternStream<JSONObject> patternedStream = CEP.pattern(keyByJsonObjDs, pattern);

        final OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {};
        SingleOutputStreamOperator<Object> filteredStream = patternedStream.flatSelect(

                timeoutTag,
                // 不符合条件（即超时）放在这里
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> map, long l, Collector<String> collector) throws Exception {
                        // map存放了所有的条件的数据，这里获取符合first的所有json数据
                        List<JSONObject> firstJsonObj = map.get("first");
                        for (JSONObject jsonObject : firstJsonObj) {
                            // 注意：在timeout方法所有参数都会被参数1中的标签标记
                            collector.collect(jsonObject.toString());
                        }
                    }
                },
                // 符合条件放在这里
                new PatternFlatSelectFunction<JSONObject, Object>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<Object> collector) throws Exception {
                        // 没有超时的数据不在统计范围之内
                    }
                });
        //通过 SideOutput 侧输出流输出超时数据
        DataStream<String> jumpDstream = filteredStream.getSideOutput(timeoutTag);
        jumpDstream.print("jump::");
        jumpDstream.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));
        env.execute();
    }
}
