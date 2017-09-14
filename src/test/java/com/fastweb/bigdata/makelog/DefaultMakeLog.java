package com.fastweb.bigdata.makelog;

import com.fastweb.bigdata.Data;
import com.fastweb.bigdata.annoation.MakeLogAnnotation;

/**
 * Created by lfq on 2017/4/11.
 */
@MakeLogAnnotation(userId = 0)
public class DefaultMakeLog implements MakeLog {

    public DefaultMakeLog() {
        System.out.println("default make log");
    }

    @Override
    public String makeLog(Data data) {
        return data.getA() + "," + data.getB() + ","+data.getC();
    }
}
