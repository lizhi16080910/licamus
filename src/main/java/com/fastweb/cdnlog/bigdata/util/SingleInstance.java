package com.fastweb.cdnlog.bigdata.util;

/**
 * Created by lfq on 2016/11/17.
 */
public class SingleInstance {
    private String str = null;

    public static SingleInstance getInstance() {
        return Singleton.INSTANCE.getInstance();
    }

    private static enum Singleton {
        INSTANCE;
        private SingleInstance instance;

        Singleton() {
            instance = new SingleInstance();
        }

        public SingleInstance getInstance() {
            return instance;
        }
    }

    public static void main(String[] args){
        SingleInstance a1= SingleInstance.getInstance();
        SingleInstance a2= SingleInstance.getInstance();
        SingleInstance a3= SingleInstance.getInstance();
        SingleInstance a4= SingleInstance.getInstance();
        System.out.println(a1);
        System.out.println(a2);
        System.out.println(a3);
        System.out.println(a4);
    }
}
