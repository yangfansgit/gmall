package com.yf.gmall.realtime.test;

import com.yf.gmall.realtime.bean.TransientSink;
import com.yf.gmall.realtime.bean.VisitorStats;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

/**
 * @author by yangfan
 * @date 2021/8/14.
 * @desc
 */
public class ReflectTest {
    public static void main(String[] args) {

        Field[] fields = VisitorStats.class.getDeclaredFields();
        for (Field field : fields) {
            TransientSink annotation = field.getAnnotation(TransientSink.class);
            System.out.println(annotation);

        }

    }
}
