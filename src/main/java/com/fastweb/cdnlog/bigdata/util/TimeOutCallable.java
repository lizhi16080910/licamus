package com.fastweb.cdnlog.bigdata.util;

import java.util.concurrent.*;

/**
 * Created by lfq on 2017/4/15.
 */
public abstract class TimeOutCallable<E> {

    private int timeout = 0; //以毫秒为单位

    abstract public E run();

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public E execute() {
        final ExecutorService exec = Executors.newFixedThreadPool(1);
        E obj = null;
        Callable<E> call = new Callable<E>() {
            public E call() throws Exception {
                //开始执行耗时操作
                return run();
            }
        };

        try {
            Future<E> future = exec.submit(call);
            obj = future.get(timeout, TimeUnit.MILLISECONDS); //任务处理超时时间设为 1 秒
            System.out.println("任务成功返回:" + obj);
        } catch (TimeoutException ex) {
            System.out.println("处理超时啦....");
            ex.printStackTrace();
        } catch (Exception e) {
            System.out.println("处理失败.");
            e.printStackTrace();
        }
        // 关闭线程池
        exec.shutdown();
        return obj;
    }
}
