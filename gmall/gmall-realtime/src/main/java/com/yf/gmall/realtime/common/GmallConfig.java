package com.yf.gmall.realtime.common;

/**
 * @author by yangfan
 * @date 2021/4/27.
 * @desc
 */
public class GmallConfig {
    public static final String HBASE_SCHEMA="GMALL2021_REALTIME";
    public static final String PHOENIX_SERVER="jdbc:phoenix:hadoop102,hadoop103,hadoop104:2185";
    public static final String CLICKHOUSE_URL="jdbc:clickhouse://hadoop102:8123/default";

}
