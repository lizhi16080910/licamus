package com.fastweb.bigdata;

/**
 * Created by lfq on 2017/4/11.
 */
public class Test2 implements CdnlogTest {
    @Override
    public String makeLog(Data data) {
        return data.getB() + "," + data.getA() + ","+data.getC();
    }
}
