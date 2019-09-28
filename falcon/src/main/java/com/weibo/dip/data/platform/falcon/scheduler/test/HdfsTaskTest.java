package com.weibo.dip.data.platform.falcon.scheduler.test;

import com.weibo.dip.data.platform.falcon.scheduler.model.MainScheduler;
import com.weibo.dip.data.platform.falcon.scheduler.task.HdfsGetTask;

import java.util.Date;

/**
 * Created by Wen on 2017/2/16.
 *
 */
public class HdfsTaskTest {
    public static void main(String[] args) {
        MainScheduler mainScheduler = MainScheduler.MainSchedulerUtil.getInstance();
        String[] jobTypes = {"hdfs"};
        mainScheduler.start(jobTypes);
        mainScheduler.submitTask(new HdfsGetTask("hello",new Date().getTime()-60000,new Date().getTime()));

        mainScheduler.executeTask(jobTypes);
    }
}
