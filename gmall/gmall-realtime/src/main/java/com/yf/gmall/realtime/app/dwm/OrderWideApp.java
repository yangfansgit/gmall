package com.yf.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yf.gmall.realtime.app.func.DimAsyncFunction;
import com.yf.gmall.realtime.bean.OrderDetail;
import com.yf.gmall.realtime.bean.OrderInfo;
import com.yf.gmall.realtime.bean.OrderWide;
import com.yf.gmall.realtime.utils.MyKafkaUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author by yangfan
 * @date 2021/5/27.
 * @desc
 */
@Data
@AllArgsConstructor
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";

        String sinkTopic = "dwm_order_wide";

        String orderWirdGroup = "order_wide_app";

        SingleOutputStreamOperator<OrderInfo>  orderInfoDs = env.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, orderWirdGroup))
                .map(new RichMapFunction<String, OrderInfo>() {
                    SimpleDateFormat simpleDateFormat;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public OrderInfo map(String s) throws Exception {
                        OrderInfo orderInfo = JSON.parseObject(s, OrderInfo.class);
                        orderInfo.setCreate_ts(simpleDateFormat.parse(orderInfo.getCreate_time()).getTime());
                        return orderInfo;
                    }
                });
        SingleOutputStreamOperator<OrderDetail> orderDetailDs = env.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, orderWirdGroup)).map(
                new RichMapFunction<String, OrderDetail>() {
                    SimpleDateFormat simpleDateFormat;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // ?????????????????????
                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public OrderDetail map(String s) throws Exception {
                        OrderDetail orderDetail = JSON.parseObject(s, OrderDetail.class);

                        orderDetail.setCreate_ts(simpleDateFormat.parse(orderDetail.getCreate_time()).getTime());

                        return orderDetail;

                    }
                }
        );
        // ??????order?????????????????????
        SingleOutputStreamOperator<OrderInfo> orderInfoWithTimeDs = orderInfoDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long l) {
                                return orderInfo.getCreate_ts();
                            }
                        }
                )
        );

        // ??????orderDetail?????????????????????
        SingleOutputStreamOperator<OrderDetail> orderDetailWithTimeDs = orderDetailDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long l) {
                                return orderDetail.getCreate_ts();
                            }
                        }
                )
        );

        /*
        ???????????????Interval join?????????????????????????????? join???intervalJoin ?????????????????????????????????
        ????????????????????????????????????????????????intervalJoin ????????????????????????????????????????????? left join???
        ???????????????????????????????????????????????????????????????????????? left join????????? intervalJoin ???
        ???????????????
        */

        // TODO 1.???????????????orderId,?????????orderId??????
        KeyedStream<OrderInfo, Long> orderInfoKeyedDs = orderInfoWithTimeDs.keyBy(orderInfo -> orderInfo.getId());

        KeyedStream<OrderDetail, Long> orderDetailKeyedDs = orderDetailWithTimeDs.keyBy(orderDetail -> orderDetail.getOrder_id());

        // TODO 2. OrderInfo interval join OrderDetail
        DataStream<OrderWide> orderWideDs = orderInfoKeyedDs.intervalJoin(orderDetailKeyedDs)
                // ???OrderDetail.order_id ?????????OrderInfo.id????????????5????????????????????????
                .between(Time.seconds(-5), Time.seconds(5))
                // ??????????????????????????????OrderWide
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> collector) throws Exception {
                        OrderWide orderWide = new OrderWide(orderInfo, orderDetail);

                        collector.collect(orderWide);
                    }
                });


        // ???????????????????????????????????????
        DataStream<OrderWide> orderWideWithUserDs = AsyncDataStream.unorderedWait(orderWideDs,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") { // ?????????????????????????????????????????????JSONObject

                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoObj) throws ParseException {
                        // ????????????????????????????????????????????????????????????
                        // ?????????????????????????????????user_age???user_gender?????????????????????????????????????????????????????????????????????
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd");
                        String birth = dimInfoObj.getString("BIRTHDAY");
                        Date date = format.parse(birth);

                        long curTs = System.currentTimeMillis();

                        long betweenMs = curTs - date.getTime();
                        Long ageLong = betweenMs / 1000L / 60L / 60L / 24L / 365L;
                        Integer age = ageLong.intValue();
                        // ???????????????????????????
                        orderWide.setUser_age(age);
                        orderWide.setUser_gender(dimInfoObj.getString("GENDER"));
                    }
                }, 60, TimeUnit.SECONDS);

        // ??????????????????
        DataStream<OrderWide> orderWideWithProviceDsteam = AsyncDataStream.unorderedWait(orderWideWithUserDs,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoObj) throws ParseException {
                        orderWide.setProvince_name(dimInfoObj.getString("NAME"));
                        orderWide.setProvince_3166_2_code(dimInfoObj.getString("ISO_3166_2"));
                        orderWide.setProvince_iso_code(dimInfoObj.getString("ISO_CODE"));
                        orderWide.setProvince_area_code(dimInfoObj.getString("AREA_CODE"));


                    }
                }, 60, TimeUnit.SECONDS);

        // ?????? SKU ??????
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDs = AsyncDataStream.unorderedWait(
                orderWideWithProviceDsteam, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //??????????????????
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3Ds = AsyncDataStream.unorderedWait(
                orderWideWithSkuDs, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                },
                60, TimeUnit.SECONDS);

        // ??????????????????
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDstream = AsyncDataStream.unorderedWait(
                orderWideWithCategory3Ds, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        // ???????????????????????? Join ??????????????????????????????????????? Kafka ??? dwm ???

        env.execute();
        orderWideWithTmDstream.map(orderWide -> JSON.toJSONString(orderWide)) .addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));
    }
}
