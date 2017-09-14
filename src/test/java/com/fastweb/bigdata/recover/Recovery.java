package com.fastweb.bigdata.recover;

import com.fastweb.cdnlog.bigdata.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by lfq on 2017/4/15.
 */
public class Recovery {

    public static ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("Usage <date> <domain file>");
            System.exit(0);
        }

        String date = args[0];
        String domainFile = args[1];

        // String date = "2017/04";
        // String domainFile = "F:\\domains_5";

        List<String> domains = FileUtil.readFile(domainFile);
        setQueue(domains); //队列的初始化

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);

        new Recovery().run(date, domainFile, fs);

        fs.close();
    }

    public void run(String date, String domainFile, FileSystem fs) {
        final ExecutorService exec = Executors.newFixedThreadPool(5);
        Runnable task1 = new MoveFileThread(date, fs,1);
        Runnable task2 = new MoveFileThread(date, fs,2);
        Runnable task3 = new MoveFileThread(date, fs,3);
        Runnable task4 = new MoveFileThread(date, fs,4);
        Runnable task5 = new MoveFileThread(date, fs,5);

        exec.submit(task1);
        exec.submit(task2);
        exec.submit(task3);
        exec.submit(task4);
        exec.submit(task5);

        exec.shutdown();

    }

    public static void setQueue(List<String> domains) {
        Recovery.queue.addAll(domains);
    }

    public static class MoveFileThread implements Runnable {
        private int taskId;
        private String date = null;
        private String destDir = "/download";
        private String srcDir = "/user/download2";
        private FileSystem fs = null;

        public MoveFileThread(String date, FileSystem fs, int taskId) {
            this.date = date;
            this.fs = fs;
            this.taskId = taskId;
        }

        @Override
        public void run() {
            int count = 0;
            while (!Recovery.queue.isEmpty()) {
                count++;
                String domain = Recovery.queue.poll();
                if (domain == null) {
                    break;
                }
                Path srcPath = new Path(srcDir + Path.SEPARATOR + domain + Path.SEPARATOR + this.date);
                Path destPath = new Path(destDir + Path.SEPARATOR + domain + Path.SEPARATOR + this.date);
                System.out.println("task id is " + this.taskId + ":" + domain);
                System.out.println("task id is " + this.taskId + " ;src path: " + srcPath.toString());
                System.out.println("task id is " + this.taskId + " ;dest path: " + destPath.toString());
                Boolean result = false;
                try {
                    Thread.sleep(20);
                    //  fs.mkdirs(destPath.getParent());
                    //  result = fs.rename(srcPath,destPath);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                // fs.rename()
            }
            System.out.println("%%%%: " + count);
        }


    }
}
