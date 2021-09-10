package com.yf.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.yf.gmall.realtime.utils.DimUtil;
import com.yf.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.text.ParseException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author by yangfan
 * @date 2021/6/21.
 * @desc 自定义维度查询异步执行函数
 *  RichAsyncFunction： 里面的方法负责异步查询
 *  DimJoinFunction： 里面的方法负责将为表和主流进行关联
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> {

    public String tableName = null;
    // 声明模板方法，调用方实现该逻辑
    public abstract String getKey(T obj);

    ExecutorService executorService = null;

    // 创建对象时传入tableName
    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    // 调用方法时，调用者实现对应的join方法
    public abstract void join(T obj, JSONObject dimInfoObj) throws ParseException;

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println ("获得线程池！ ");
        // 获取连接池对象
        executorService = ThreadPoolUtil.getInstance();

    }

    // obj为传过来的bean对象
    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try{
                    long start = System.currentTimeMillis();
                    // 获取查询Key
                    String key = getKey(obj);
                    // 获取维度信息
                    JSONObject dimInfoObj = DimUtil.getDimInfo(tableName, key);
                    if(dimInfoObj!=null){
                        //
                        join(obj,dimInfoObj);
                    }
                    long end = System.currentTimeMillis();
                    System.out.println("异步耗时："+(end-start)+"毫秒");
                    // 回调结果向下传递
                    resultFuture.complete(Arrays.asList(obj));
                } catch (Exception e) {
                    System.out.println(String.format(tableName+"异步查询异常. %s", e));
                    e.printStackTrace();
                }
            }
        });
    }

}
