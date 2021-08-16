package com.yf.gmall.realtime.bean;

/**
 * @author by yangfan
 * @date 2021/8/15.
 * @desc  向 ClickHouse 写入数据的时候，如果有字段数据不需要传输，可以用该注解标记
 */

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.lang.annotation.ElementType.FIELD;

@Target(FIELD)
@Retention(RUNTIME)

public @interface TransientSink {

}
