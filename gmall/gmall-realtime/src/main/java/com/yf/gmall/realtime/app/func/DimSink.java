package com.yf.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.yf.gmall.realtime.common.GmallConfig;
import com.yf.gmall.realtime.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author by yangfan
 * @date 2021/4/30.
 * @desc 通过 Phoenix 向 Hbase 表中写数据
 */
public class DimSink extends RichSinkFunction<JSONObject> {
    private Connection conn;
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        // 初始化Connection对象
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject jsonObject, Context context)  {
        // 获取要插入的表名、字段 
        String tableName = jsonObject.getString("sink_table");
        JSONObject colsJson = jsonObject.getJSONObject("data");

        if (jsonObject != null && jsonObject.size() > 0) {
            String upsertSql = genUpsertSql(tableName.toUpperCase(), jsonObject.getJSONObject("data"));
            try {
                PreparedStatement ps = conn.prepareStatement(upsertSql);
                ps.execute();
                // phoenix需要单独进行commit
                conn.commit();
                ps.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                throw new RuntimeException("执行sql失败");
            }

        }

        if(jsonObject.getString("type").equals("update") ||
                jsonObject.getString("type").equals("delete")){
            // key为id对应的值
            DimUtil.deleteCached(tableName,jsonObject.getJSONObject("data").getString("id"));
        }

    }

    // 处理执行sql
    private String genUpsertSql(String tableName, JSONObject jsonObject) {
        // 字段名Set
        Set<String> colsSet = jsonObject.keySet();
        // 字段value的Set
        Collection<Object> values = jsonObject.values();

        String upsertSql = "upsert into " +  GmallConfig.HBASE_SCHEMA + "." + tableName + "("
                + StringUtils.join(colsSet,",") + ")";

        String valuesSql = " values ('" + StringUtils.join(jsonObject.values(), "','") + "')";

        System.out.println(upsertSql + valuesSql);


        return upsertSql + valuesSql;
    }


}
