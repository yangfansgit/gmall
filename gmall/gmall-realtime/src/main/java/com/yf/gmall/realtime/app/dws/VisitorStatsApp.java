package com.yf.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yf.gmall.realtime.bean.VisitorStats;
import com.yf.gmall.realtime.utils.ClickhouseUtil;
import com.yf.gmall.realtime.utils.DateTimeUtil;
import com.yf.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple4;

import java.time.Duration;
import java.util.Date;

/**
 * @author by yangfan
 * @date 2021/8/1.
 * @desc 访客主题宽表计算
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //检查点 CK 相关设置
        /*env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop102:8020/gmall/flink/checkpoint/VisitorStatsApp");
        env.setStateBackend(fsStateBackend);*/

        String groupId = "visitor_stats_app";
        //TODO 1.从 Kafka 的 pv、uv、跳转明细主题中获取数据
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        DataStreamSource<String> pageViewDs = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> uniqueVisitDs = env.addSource(MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> userJumpDetailDs = env.addSource(MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId));

        // 对pv逻辑的计算
        SingleOutputStreamOperator<VisitorStats> pvVisitorStatsDs = pageViewDs.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                VisitorStats visitorStats = new VisitorStats(
                        // 窗口开始结束时间在改方法中无法获得，后续进行处理
                        "",
                        "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        0L,
                        1L,
                        0L,
                        0L,
                        jsonObject.getJSONObject("page").getLong("during_time"),
                        jsonObject.getLong("ts")

                );

                return visitorStats;
            }
        });

        // 对uv逻辑的计算
        SingleOutputStreamOperator<VisitorStats> uvVisitorStatsDs = uniqueVisitDs.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                VisitorStats visitorStats = new VisitorStats(
                        // 窗口开始结束时间在改方法中无法获得，后续进行处理
                        "",
                        "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        1L,
                        0L,
                        0L,
                        0L,
                        0L,
                        jsonObject.getLong("ts")

                );
                return visitorStats;
            }
        });

        // 对sv逻辑的计算，sv-会话数，如果页面没有last_page_id，则认为是一个su，因此既需要过滤，又需要转换
        SingleOutputStreamOperator<VisitorStats> svVisitorStatsDs = pageViewDs.process(new ProcessFunction<String, VisitorStats>() {
            @Override
            public void processElement(String s, Context context, Collector<VisitorStats> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || lastPageId.length() == 0) {
                    VisitorStats visitorStats = new VisitorStats(
                            // 窗口开始结束时间在改方法中无法获得，后续进行处理
                            "",
                            "",
                            jsonObject.getJSONObject("common").getString("vc"),
                            jsonObject.getJSONObject("common").getString("ch"),
                            jsonObject.getJSONObject("common").getString("ar"),
                            jsonObject.getJSONObject("common").getString("is_new"),
                            0L,
                            0L,
                            1L,
                            0L,
                            0L,
                            jsonObject.getLong("ts")

                    );
                    collector.collect(visitorStats);
                }

            }
        });

        // 转换跳转流
        SingleOutputStreamOperator<VisitorStats> userJumpVisitorStatsDs = userJumpDetailDs.map(json -> {
            JSONObject jsonObj = JSON.parseObject(json);
            return new VisitorStats("", "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    0L, 0L, 0L, 1L, 0L, jsonObj.getLong("ts"));
        });


        // 四条流union 起来
        DataStream<VisitorStats> unionDs = pvVisitorStatsDs.union(uvVisitorStatsDs, svVisitorStatsDs, userJumpVisitorStatsDs);

        // 设置水位线
        SingleOutputStreamOperator<VisitorStats> unionDsWithWaterMark = unionDs.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
            @Override
            public long extractTimestamp(VisitorStats visitorStats, long l) {
                return visitorStats.getTs();
            }
        }));


        // 分组，需要对版本、渠道、地区、新老客进行分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> unionDsWithKeyBy = unionDsWithWaterMark.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                return new Tuple4<>(visitorStats.getVc(),
                        visitorStats.getCh(),
                        visitorStats.getAr(),
                        visitorStats.getIs_new());
            }
        });


        // 开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowStream = unionDsWithKeyBy.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        // 聚合
        SingleOutputStreamOperator<VisitorStats> visitorStatsDs = windowStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
                //把度量数据两两相加
                stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
                stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                return stats1;
            }
        }, new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void process(Tuple4<String, String, String, String> stringStringStringStringTuple4,
                                Context context, Iterable<VisitorStats> iterable, Collector<VisitorStats> collector) throws Exception {
                for (VisitorStats visitorStats : iterable) {
                    // 补充时间字段
                    String startDate = DateTimeUtil.toYMDhms(new Date(context.window().getStart()));
                    String endDate = DateTimeUtil.toYMDhms(new Date(context.window().getEnd()));
                    visitorStats.setStt(startDate);
                    visitorStats.setEdt(endDate);
                    collector.collect(visitorStats);
                }
            }
        });
        // 一本正经的注释
        visitorStatsDs.addSink(ClickhouseUtil.getJDBCSink("insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        visitorStatsDs.print("reduce:");

        env.execute("visitStateApp");

    }
}
