package com.yf.gmall.realtime.test;

import com.yf.gmall.realtime.common.GmallConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author by yangfan
 * @date 2021/8/15.
 * @desc
 */
public class JDBCTest {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        Connection connection = DriverManager.getConnection(GmallConfig.CLICKHOUSE_URL);

        System.out.println(connection);


    }
}
