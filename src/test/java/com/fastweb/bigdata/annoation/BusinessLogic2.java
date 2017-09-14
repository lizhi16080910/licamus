/*
 * This Class demonstrates use of Todo annotation defined in Todo.java
 * 
 * @author Yashwant Golecha (ygolecha@gmail.com)
 * @version 1.0
 * 
 */

package com.fastweb.bigdata.annoation;

@Todo2
public class BusinessLogic2 {
    public BusinessLogic2() {
        super();
        System.out.println("ssss");
    }
    
    public void compltedMethod() {
        System.out.println("This method is complete");
    }    
    
    @Todo(priority = Todo.Priority.HIGH)
    public void notYetStartedMethod() {
        // No Code Written yet
    }
    
    @Todo(priority = Todo.Priority.MEDIUM, author = "Uday", status = Todo.Status.STARTED)
    public void incompleteMethod1() {
        //Some business logic is written
        //But its not complete yet
    }

    @Todo(priority = Todo.Priority.LOW, status = Todo.Status.STARTED )
    public void incompleteMethod2() {
        //Some business logic is written
        //But its not complete yet
    }
}
