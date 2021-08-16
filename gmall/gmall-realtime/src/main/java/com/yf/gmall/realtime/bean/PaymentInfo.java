package com.yf.gmall.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @author by yangfan
 * @date 2021/7/12.
 * @desc 支付信息实体类
 */
@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;
}
