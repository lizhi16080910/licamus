package com.fastweb.bigdata.makelog;

import com.fastweb.bigdata.Data;
import com.fastweb.bigdata.annoation.MakeLogAnnotation;
import com.fastweb.bigdata.util.ClassUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lfq on 2017/4/11.
 */
@MakeLogAnnotation(userId = 2)
public class MakeLog2 implements MakeLog {
    public MakeLog2() {
        System.out.println("make log 2");
    }

    @Override
    public String makeLog(Data data) {
        return null;
    }

    public static void main(String[] args) throws IllegalAccessException, InstantiationException {

        Data data = new Data();
        List<Class<?>> classes = ClassUtil.getClasses("com.fastweb.bigdata.makelog");

        Map<Integer,MakeLog> map = new HashMap<>();
        for (Class<?> a : classes) {
            MakeLogAnnotation annotation = a.getAnnotation(MakeLogAnnotation.class);
            if (annotation != null) {
                int userid = annotation.userId();
                MakeLog makeLog = (MakeLog) a.newInstance();
                map.put(userid,makeLog);
            }
        }
        System.out.println(map.get(1).makeLog(data));
        System.out.println(map.get(0).makeLog(data));
        System.out.println(map.get(2).makeLog(data));
        System.out.println(map.get(2).makeLog(data));
        System.out.println(map.get(2).makeLog(data));

    }

}
