package com.weibo.dip.data.platform.falcon.scheduler.test;

import com.weibo.dip.data.platform.falcon.scheduler.model.Worker;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Wen on 2017/1/19.
 *
 */
public class StartWorker {
    private static  final Logger LOGGER = LoggerFactory.getLogger(StartWorker.class);
    private static  final String CONNECTSTRING = "10.210.136.61:2181";
    private  static final String PATH = "/test/leader";
    private static void start() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(CONNECTSTRING,new ExponentialBackoffRetry(1000,3));
        LeaderLatch leader = new LeaderLatch(client,PATH,CONNECTSTRING);
        client.start();
        leader.start();
        while (!leader.hasLeadership()){
            LOGGER.info("I'm not the leader");
        }
        LOGGER.info(leader.getId()+" is leader.Let's start the worker");
        Worker worker = new Worker();
        worker.startGetRecentFinishedFileListTask(worker.getDataSetList(),200);
        LOGGER.info("Worker has been started!");
        LOGGER.info("start update task");
        worker.updateTaskSchedule();
        LOGGER.info("UpdateTask has been started!");
    }

    public static void main(String[] args) throws Exception {
        start();
    }



}
