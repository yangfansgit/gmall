package com.yf.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.yf.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;
import org.codehaus.jackson.map.util.BeanUtil;


import java.sql.*;
import java.util.ArrayList;
import java.util.List;


/**
 * @author by yangfan
 * @date 2021/6/5.
 * @desc
 */
public class PhoenixUtil {
    public static Connection conn = null;

    public static void queryInit() {

        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (Exception throwables) {
            throwables.printStackTrace();
        }
    }
    public static <T>List<T> queryList(String sql,Class<T> clazz){
        // 初始化连接对象
        if(conn == null) {
            queryInit();
        }
        PreparedStatement ps = null;
        ResultSet rs = null;
        List objList = new ArrayList();
        // 查询结果集
        try {
            ps = conn.prepareStatement(sql);
            // 获取原数据信息（列名）
            ResultSetMetaData metaData = ps.getMetaData();
            // 获取查询结果集
            rs = ps.executeQuery();
            Object o = clazz.newInstance();
            // 封装对象
            while(rs.next()) {
                // System.out.println(rs.getInt(1) + "-------------->");
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    // 使用BeanUtils封装结果集
                    BeanUtils.setProperty(o,metaData.getColumnName(i),rs.getObject(i));
                }
                objList.add(o);
            }


        } catch (Exception throwables) {
            throwables.printStackTrace();
        }

        return objList;
    }

    public static void main(String[] args) {
        String sql = "SELECT * FROM DIM_USER_INFO where id = '879'";
        List<JSONObject> jsonObjects = queryList(sql, JSONObject.class);

        System.out.println(jsonObjects);
    }
}
