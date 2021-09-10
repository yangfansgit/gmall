package com.yf.gmall.realtime.app.dwd;

/**
 * @author by yangfan
 * @date 2021/6/21.
 * @desc
 */
public abstract class AbstractTest {
    public abstract void t1();

    public void t2(){
        System.out.println("t2 mothod");

    }

    public static void main(String[] args) {
        new AbstractTest() {
            @Override
            public void t1() {

            }
        }.t2();
    }
}
