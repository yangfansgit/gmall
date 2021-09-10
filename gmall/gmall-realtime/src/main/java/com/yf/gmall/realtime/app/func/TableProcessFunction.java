package com.yf.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.yf.gmall.realtime.bean.TableProcess;
import com.yf.gmall.realtime.common.GmallConfig;
import com.yf.gmall.realtime.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @author by yangfan
 * @date 2021/4/24.
 * @desc用于对业务数据进行分流处理的自定义处理函数
 */
public class TableProcessFunction extends ProcessFunction<JSONObject,JSONObject> {

    //Conn连接对象
    private Connection conn;

    // 侧输出流标签
    private OutputTag<JSONObject> outPutTag;

    // 存放配置信息的map
    private Map<String,TableProcess> tableProcessMap = new HashMap<String,TableProcess>();

    // 存放已经操作过的表Set集合
    Set<String>  existsTableSet = new HashSet<String>();

    public TableProcessFunction(OutputTag<JSONObject> outPutTag) {
        this.outPutTag = outPutTag;
    }

    @Override
    public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
        // 进行分流处理，主流为实时表，侧流为维表

        // 对字段进行过滤

        String table = jsonObject.getString("table");
        String type = jsonObject.getString("type");
        // 对于离线数据，maxwell采集的到type为为 bootstrap-insert，需要进行特殊处理
        if("bootstrap-insert".equals(type)) {
            type = "insert";
        }

        if(tableProcessMap != null && tableProcessMap.size() > 0) {
            // 获得MAP中key
            String key = table + ":" + type;
            TableProcess tableProcess = tableProcessMap.get(key);
            if(tableProcess != null) {
                // 获取数据库中该表的行
                String sinkColumns = tableProcess.getSinkColumns();
                jsonObject.put("sink_table",tableProcess.getSinkTable());
                if(sinkColumns != null && sinkColumns.length() > 0) {
                    filterColumn(jsonObject.getJSONObject("data"),sinkColumns);
                }
            } else {
                System.out.println("This is not key:" + key);

            }

            if(tableProcess != null) {
                // 实时数据输出到kafka，主流
                if(TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                    collector.collect(jsonObject);
                }else if(TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                    context.output(outPutTag,jsonObject);
                }
            }
        }

    }

    // 过滤的逻辑
    private void filterColumn(JSONObject colsJson,String cols) {
        // 列名转为list集合，便于比较
        List<String> colsList = Arrays.asList(cols.split(","));

        // 将json对象转为实体
        Set<Map.Entry<String, Object>> colSet = colsJson.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = colSet.iterator();

        while(iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            // 得到当前key的value，过滤无用字段
            if(!colsList.contains(entry.getKey())) {
                iterator.remove();
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 创建数据库连接对象
        // phoenix驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        // 初始化Connection对象
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        // 初始化的方法，调用一次查询数据库的方法
        refreshMeta();
        // 由于维表也会变化，因此需要5秒钟重复查询一次数据库
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                refreshMeta();
            }
            // 5秒后执行，每隔5秒重复执行
        },5000,5000);
    }

    // 从数据库查询配置信息
    private void refreshMeta() {
        System.out.println("开始查询配置表");

        List<TableProcess> tableProcessList = MySQLUtil.queryList("select * from table_process",TableProcess.class,true);

        for (TableProcess tableProcess : tableProcessList) {
            String sourceTable = tableProcess.getSourceTable();
            String operateType = tableProcess.getOperateType();
            String sinkType = tableProcess.getSinkType();
            String sinkTable = tableProcess.getSinkTable();
            String sinkColumns = tableProcess.getSinkColumns();
            String sinkPk = tableProcess.getSinkPk();
            String sinkExtend = tableProcess.getSinkExtend();

            // 将source表与操作类型作为唯一Key
            String key = sourceTable + ":" + operateType;
            // 放入map，为了后续查询
            tableProcessMap.put(key,tableProcess);

            // 对于insert操作并且sinkHbase操作，需要在Hbase判断该表是否存在，存在不处做处理
            if(TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(operateType)) {
                // 判断缓存是否存在该表
                boolean notExists = existsTableSet.add(sinkTable);

                // 不存在执行创建sql的语句
                if(notExists) {
                    checkTable(sinkTable,sinkColumns,sinkPk,sinkExtend);
                }
            }
        }
        if (tableProcessMap == null || tableProcessMap.size() == 0) {
            throw new RuntimeException("缺少处理信息");
        }

    }

    // 检查并在hbase中创建该表
    private void checkTable(String tableName,String fields,String pk,String ext) {
        // 对于pk与ext为null时需要赋默认值
        if(pk == null) {
            pk = "id";
        }
        if(ext == null) {
            ext = "";
        }
        // 缓存中没有该表不代表hbase没有，可能由于程序挂了，因此需要进行not exists 操作
        StringBuilder createTableSql = new StringBuilder("create table if not exists "
                + GmallConfig.HBASE_SCHEMA + "." + tableName + "(");
        String[] col = fields.split(",");
        // 遍历拼接建表字段
        for (int i = 0;i < col.length;i++) {
            // 主键拼接primary key
            if(pk.equals(col[i])) {
                // info为列族
                createTableSql.append(col[i]).append(" varchar primary key ");
            }else {
                // 普通字段
                createTableSql.append("info.").append(col[i]).append(" varchar");
            }
            // 不是最后一个字段需要拼接逗号
            if(i < col.length - 1) {
                createTableSql.append(",");
            }
        }
        createTableSql.append(")");
        System.out.println("创建sql:"+createTableSql.toString());
        createTableSql.append(ext);

        // 执行sql，JDBC
        try {
            PreparedStatement ps = conn.prepareStatement(createTableSql.toString());
            ps.execute();
            ps.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            throw new RuntimeException("建表失败！！！");
        }
    }
}
