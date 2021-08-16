package com.yf.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author by yangfan
 * @date 2021/6/9.
 * @desc 查询维度的工具类
 */
public class DimUtil {
    //直接从 Phoenix 查询，没有缓存
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>...  colNameAndValue) {

        String wheresql = " where ";
        for (int i = 0; i < colNameAndValue.length; i++) {
            //获取查询列名以及对应的值
            Tuple2<String, String> nameValueTuple = colNameAndValue[i];
            String fieldName = nameValueTuple.f0;
            String fieldValue = nameValueTuple.f1;
            if (i > 0) {
                wheresql += " and ";
            }
            wheresql += fieldName + "='" + fieldValue + "'";
        }
        //组合查询 SQL
        String sql = "select * from " + tableName + wheresql;
        System.out.println("查询维度 SQL:" + sql);
        JSONObject dimInfoJsonObj = null;
        List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
        if (dimList != null && dimList.size() > 0) {
            //因为关联维度，肯定都是根据 key 关联得到一条记录
            dimInfoJsonObj = dimList.get(0);
        }else{
            System.out.println("维度数据未找到:" + sql);
        }
        return dimInfoJsonObj;
    }

    /* 旁路缓存，增加查询效率
        类型      String
        key      dim:表名:值   例如：dim:DIM_BASE_TRADMARK:10
        valyue   第一次从PhoenixUtil查询出来的jsonObkect，为了是redis中的内容和从hbase中查询出封装后的结果一致
     */
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... colNameAndValue) {
        String where = " where ";
        String redisKey = "";
        for (int i = 0; i < colNameAndValue.length; i++) {
            String colName = colNameAndValue[i].f0;
            String colValue = colNameAndValue[i].f1;
            if(i > 0) {
                where += " and";
                redisKey += "_";
            }

            where += colName + " = '" + colValue + "'";
            redisKey += colValue;
        }
        Jedis jedis = null;
        String dimJson = null;
        JSONObject dimInfo = null;
        String key = "dim:" + tableName.toLowerCase() + ":" + redisKey;
        try {
            // 从连接池获得连接
            jedis = RedisUtil.getJedis();
            // 通过 key 查询缓存
            dimJson = jedis.get(key);
        } catch (Exception e) {
            System.out.println("缓存异常！");
            e.printStackTrace();
        }

        if (dimJson != null) {
            dimInfo = JSON.parseObject(dimJson);
        } else {
            String sql = "select * from " + tableName + where;
            System.out.println("查询维度 sql:" + sql);
            List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
            if (dimList.size() > 0) {
                dimInfo = dimList.get(0);
                if (jedis != null) {
                    //把从数据库中查询的数据同步到缓存
                    jedis.setex(key, 3600 * 24, dimInfo.toJSONString());
                }
            } else {
                System.out.println("维度数据未找到：" + sql);
            }
        }
        if (jedis != null) {
            jedis.close();
            System.out.println("关闭缓存连接 ");
        }
        return dimInfo;
    }
    //先从 Redis 中查，如果缓存中没有再通过 Phoenix 查询 固定 id 进行关联
    public static JSONObject getDimInfo(String tableName, String id) {
        Tuple2<String, String> kv = Tuple2.of("id", id);
        return getDimInfo(tableName, kv);
    }

    // 当数据发生变化时，清除缓存
    public static void deleteCached( String tableName, String idValue){
        String redisKey = "dim:" + tableName.toLowerCase() + ":" + idValue;
        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            // 通过key删除缓存
            jedis.del(redisKey);
            System.out.println("删除Key:"+redisKey);
        }catch (Exception e) {

        }finally {
            jedis.close();
        }
    }
    public static void main(String[] args) {
       // System.out.println(getDimInfo("DIM_USER_INFO",new Tuple2<>("id","960")));
    }

}
