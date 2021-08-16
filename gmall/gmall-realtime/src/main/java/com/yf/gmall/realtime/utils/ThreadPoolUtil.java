package com.yf.gmall.realtime.utils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author by yangfan
 * @date 2021/6/21.
 * @desc 线程池工具类
 */
public class ThreadPoolUtil {
    public static ThreadPoolExecutor pool;
    private ThreadPoolUtil() {

    }

    /*
        获取单例的线程池对象，懒汉式，需要保证线程安全
        corePoolSize:初始线程数量，指定了线程池中的线程数量，它的数量决定了添加的任务是开辟新的线程去执行，还是放到 workQueue
        任务队列中去；
        maximumPoolSize:指定了线程池中的最大线程数量，这个参数会根据你使用的 workQueue 任务队列的类型，决定线程
        池会开辟的最大线程数量；
        keepAliveTime:当线程池中空闲线程数量超过 corePoolSize 时，多余的线程会在多长时间内被销毁；
        unit:keepAliveTime 的单位
        workQueue:任务队列，被添加到线程池中，但尚未被执行的任务，LinkedBlockingDeque为无界的队列，ArrayBlockingQueue为有界队列，必须指定最大参数
    */
    public static ThreadPoolExecutor getInstance(){
        if(pool == null) {
            synchronized(ThreadPoolUtil.class) {
                if(pool == null) {
                    System.out.println ("开辟程池！！！！！");
                    pool = new ThreadPoolExecutor(4,20,300, TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>(Integer.MAX_VALUE));
                }
            }
        }
        return pool;
    }
}
