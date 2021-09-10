package com.yf.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yf.gmall.realtime.bean.OrderInfo;
import com.yf.gmall.realtime.bean.OrderWide;
import com.yf.gmall.realtime.bean.PaymentInfo;
import com.yf.gmall.realtime.bean.PaymentWide;
import com.yf.gmall.realtime.utils.DateTimeUtil;
import com.yf.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author by yangfan
 * @date 2021/7/12.
 * @desc
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        // 读取 dwd_payment_info
        DataStreamSource<String> paymentJsonDs = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId));
        // 对读取的支付数据进行转换
        SingleOutputStreamOperator<PaymentInfo> paymentDs = paymentJsonDs.map(new MapFunction<String, PaymentInfo>() {
            @Override
            public PaymentInfo map(String s) throws Exception {
                PaymentInfo paymentInfo = JSON.parseObject(s, PaymentInfo.class);
                return paymentInfo;
            }
        });

        // 读取dwm_order_wide
        DataStreamSource<String> orderWideJsonDs = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));
        // 对读取的订单宽表数据进行转换
        SingleOutputStreamOperator<OrderWide> orderWideDs = orderWideJsonDs.map(new MapFunction<String, OrderWide>() {
            @Override
            public OrderWide map(String s) throws Exception {
                OrderWide orderWide = JSON.parseObject(s, OrderWide.class);
                return orderWide;
            }
        });


        // 主流是支付表，设置水位线
        SingleOutputStreamOperator<PaymentInfo> PaymentInfoDsWithWaterMark = paymentDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo paymentInfo, long l) {
                                return DateTimeUtil.toTs(paymentInfo.getCreate_time());
                            }
                        })
        );

        // 订单宽表设置水位线
        SingleOutputStreamOperator<OrderWide> OrderWideDsWithWaterMark = orderWideDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide orderWide, long l) {
                                return DateTimeUtil.toTs(orderWide.getCreate_time());
                            }
                        })
        );

        // 对orderId进行分组
        KeyedStream<PaymentInfo, Long> paymentInfoKeyDs = PaymentInfoDsWithWaterMark.keyBy(paymentInfo -> paymentInfo.getOrder_id());
        KeyedStream<OrderWide, Long> orderWideLongKeyedStream = OrderWideDsWithWaterMark.keyBy(orderId -> orderId.getOrder_id());

        // intervalJoin
        SingleOutputStreamOperator<PaymentWide> paymentWideDs = paymentInfoKeyDs.intervalJoin(orderWideLongKeyedStream).
                // 下单到支付应该选择最大支付时间
                between(Time.seconds(-1800), Time.seconds(5)).process(
                new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context context, Collector<PaymentWide> collector) throws Exception {
                        PaymentWide paymentWide = new PaymentWide(paymentInfo,orderWide);
                        collector.collect(paymentWide);
                    }
                }
        );

        SingleOutputStreamOperator<String> paymentWideStringDstream =
                paymentWideDs.map(paymentWide -> JSON.toJSONString(paymentWide));
        paymentWideStringDstream.print("pay:");
        paymentWideStringDstream.addSink(
                MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        env.execute();

    }
}
