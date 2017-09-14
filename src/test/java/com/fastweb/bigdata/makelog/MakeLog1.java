package com.fastweb.bigdata.makelog;

import com.fastweb.bigdata.Data;
import com.fastweb.bigdata.annoation.MakeLogAnnotation;

import java.lang.annotation.Annotation;

/**
 * Created by lfq on 2017/4/11.
 */
@MakeLogAnnotation(userId = 1)
public class MakeLog1 implements MakeLog {

    public MakeLog1() {
        System.out.println("make log 1");
    }

    @Override
    public String makeLog(Data data) {
        return data.getB() + "," + data.getA() + "," + data.getC();
    }

    public static void main(String[] args){
        Annotation annotation = new MakeLog1().getClass().getAnnotation(MakeLogAnnotation.class);
        System.out.println(annotation.annotationType());
    }
}
