package com.weibo.dip.data.platform.falcon.scheduler.test;

import com.weibo.dip.data.platform.falcon.scheduler.model.MainScheduler;
import com.weibo.dip.data.platform.falcon.scheduler.model.Task;

/**
 * Created by Wen on 2017/2/17.
 *
 */
public class TestScheduler {
    private static final MainScheduler MAIN_SCHEDULER = MainScheduler.MainSchedulerUtil.getInstance();
    public static class GetSchedulerStatus implements Runnable {

        @Override
        public void run() {
            while (true) {
                MAIN_SCHEDULER.showStatus();
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class SubmitTask1 implements Runnable {
        static int i =0;
        @Override
        public void run() {
            while (true) {

                Task task = new TestTask();
                task.setJobName("Job-" + Integer.toString(i++));
                task.setJobType("1");
                MAIN_SCHEDULER.submitTask(task);
                System.out.println("Submit " + task.getJobName());
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class SubmitTask2 implements Runnable {
        static int i =0;
        @Override
        public void run() {
            while (true) {

                Task task = new TestTask();
                task.setJobName("Job-" + Integer.toString(i++));
                task.setJobType("2");
                MAIN_SCHEDULER.submitTask(task);
                System.out.println("Submit " + task.getJobName());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {

        String[] jobTypes = {"1","2"};
        MAIN_SCHEDULER.start(jobTypes);
        new Thread(new GetSchedulerStatus()).start();
        for (int i = 0; i<100;i++) {
            Task task = new TestTask();
            task.setJobName("Job-" + Integer.toString(i) + "-JobType : " + jobTypes[i%2]);
            task.setJobType( jobTypes[i%2]);
            MAIN_SCHEDULER.submitTask(task);
        }

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        MAIN_SCHEDULER.executeTask(jobTypes);
        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new Thread(new SubmitTask1()).start();
        new Thread(new SubmitTask2()).start();
    }
}
