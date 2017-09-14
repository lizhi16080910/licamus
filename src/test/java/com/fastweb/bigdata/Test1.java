package com.fastweb.bigdata;

/**
 * Created by lfq on 2017/4/11.
 */
public class Test1 implements CdnlogTest{

    @Override
    public String makeLog(Data data) {
        return data.getA() + "," + data.getB() + ","+data.getC();
    }
}
