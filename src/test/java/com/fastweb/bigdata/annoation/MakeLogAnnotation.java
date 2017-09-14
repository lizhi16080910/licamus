package com.fastweb.bigdata.annoation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by lfq on 2017/4/11.
 */

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface MakeLogAnnotation {
//    public enum Priority {LOW, MEDIUM, HIGH}
//    public enum Status {STARTED, NOT_STARTED}
//    String author() default "Yash";
//    Priority priority() default Priority.LOW;
//    Status status() default Status.NOT_STARTED;

    /**
     * 数据表名称注解，默认值为类名称
     * @return
     */
    public int userId() default 0;
}
