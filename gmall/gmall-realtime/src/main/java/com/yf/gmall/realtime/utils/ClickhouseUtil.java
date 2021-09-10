package com.yf.gmall.realtime.utils;

import com.yf.gmall.realtime.bean.TransientSink;
import com.yf.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author by yangfan
 * @date 2021/8/14.
 * @desc 操作 ClickHouse 的工具类
 */
public class ClickhouseUtil {
    public static <T> SinkFunction getJDBCSink(String sql) {
        // JdbcSink是flink提供的一个针对于jdbc的sink方法
        SinkFunction<T> sink = JdbcSink.sink(sql,
                // ps:数据库操作对象，obj:流中一条数据
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement ps, T obj) throws SQLException {

                        // 通过反射获取属性，包含private
                        Field[] fields = obj.getClass().getDeclaredFields();
                        int skipOffset = 0;
                        for (int i = 0; i < fields.length; i++) {
                            Field field = fields[i];
                            // 开启运行访问私有属性
                            field.setAccessible(true);
                            try {
                                TransientSink annotation = field.getAnnotation(TransientSink.class);
                                if(annotation != null) {
                                    skipOffset ++;
                                    continue;
                                } else {
                                    // 设置占位符参数
                                    ps.setObject(i+1 - skipOffset,field.get(obj));
                                }
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }


                        }
                    }
                },
                // 设置执行的选项，如设置批处理的大小
                new JdbcExecutionOptions.Builder().withBatchSize(10).build(),
                // 设置连接信息，如连接地址，驱动等信息
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build()
        );

        return sink;
    }
}

