package com.yf.gmall.com.yf.gmall;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


import java.io.IOException;

/**
 * @author by yangfan
 * @date 2021/3/22.
 *
 * @Slf4j lombook注解，辅助第三方记录日志框架
 */

@RestController // 表示返回结果全是字符串，不需要在每个方法加@ResponseBody
@Slf4j
public class loggerController {
    // KafkaTemplate是springBoot集成的操作kafka的类，需要注入进去
    @Autowired
    KafkaTemplate kafkaTemplate;

    // @Slf4j 相当于 private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(loggerController.class);
    @RequestMapping("/applog") // 请求地址为applog执行下面方法
    public String looger(@RequestParam("param") String jsonLog) throws IOException { // @RequestParam("param") 表示接收一个参数赋值给jsonLog
        // @Slf4j会自动生成一个log对象，使用info的形式打印日志
        log.info(jsonLog);

        // 将生成的日志发送的kafka对应的主题中
        kafkaTemplate.send("ods_base_log",jsonLog);
        return "success";

    }
}
