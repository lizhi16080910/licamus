/*
 * This Class demonstrates use of annotations using reflection.
 * 
 * @author Yashwant Golecha (ygolecha@gmail.com)
 * @version 1.0
 * 
 */

package com.fastweb.bigdata.annoation;

import java.lang.annotation.Annotation;

public class TodoReport {
    public TodoReport() {
        super();
    }

    public static void main(String[] args) {
        getTodoReportForBusinessLogic();
    }

    /**
     * This method iterates through all messages of BusinessLogic class and fetches annotations defined on each of them.
     * After that it displays the information from annotation accordingly.
     */
    private static void getTodoReportForBusinessLogic() {

//        Class businessLogicClass = BusinessLogic.class;
//        for(Method method : businessLogicClass.getMethods()) {
//            Todo todoAnnotation = (Todo)method.getAnnotation(Todo.class);
//            if(todoAnnotation != null) {
//                System.out.println(" Method Name : " + method.getName());
//                System.out.println(" Author : " + todoAnnotation.author());
//                System.out.println(" Priority : " + todoAnnotation.priority());
//                System.out.println(" Status : " + todoAnnotation.status());
//                System.out.println(" --------------------------- ");
//            }
//        }

        ClassLoader loader = ClassLoader.getSystemClassLoader();


        Class businessLogicClass2 = BusinessLogic2.class;
        Package pa = Package.getPackage("com.lifq.annotation");

        System.out.println(pa);

        System.out.println(pa.getAnnotations().length);

        Todo2 an = (Todo2)pa.getAnnotation(Todo2.class);
       // System.out.println(an.author());
        for(Annotation ann :pa.getAnnotations()){

            Todo2 anb = (Todo2)ann;
            System.out.println(anb.author());
            System.out.println(anb.status());
        }
//        Todo2 ann = (Todo2) businessLogicClass2.getAnnotation(Todo2.class);
//        if (ann != null) {
//            System.out.println(ann.author());
//            System.out.println(ann.status());
//        }

    }
}
