package com.weibo.dip.data.platform.falcon.scheduler.service;

import com.google.gson.Gson;
import com.weib.dip.data.platform.services.client.ConfigService;
import com.weib.dip.data.platform.services.client.model.ConfigEntity;
import com.weib.dip.data.platform.services.client.util.ServiceProxyBuilder;
import com.weibo.dip.data.platform.falcon.scheduler.model.GetRecentFinishedFileListTask;
import com.weibo.dip.data.platform.falcon.scheduler.model.Worker;
import org.apache.commons.lang.StringUtils;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Wen on 2017/1/20.
 *
 */
public class GetDataSetList implements Job {
    private final static Logger LOGGER = LoggerFactory.getLogger(GetDataSetList.class);
    private Gson gson = new Gson();

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        ConfigService configService = ServiceProxyBuilder.buildLocalhost(ConfigService.class);
        try {
            List<String> oldDataSetList = Arrays.asList(configService.get("DataSetList").split(","));
            Worker additionalTaskWorker = new Worker();
            List<String> newDataSetList = additionalTaskWorker.getDataSetList();
            if (!oldDataSetList.equals(newDataSetList)){
                ConfigEntity configEntity = new ConfigEntity();
                configEntity.setKey("DataSet");
                configEntity.setValue(StringUtils.join(newDataSetList,","));

                List<String> shouldStartTaskList = new ArrayList<>(newDataSetList);
                shouldStartTaskList.removeAll(oldDataSetList);
                List<String> shouldStopTaskList = new ArrayList<>(oldDataSetList);
                oldDataSetList.removeAll(newDataSetList);

                additionalTaskWorker.startGetRecentFinishedFileListTask(shouldStartTaskList,200);
                //清理:完成时清理
                for (String shouldStopTask : shouldStopTaskList){
                    if (configService.exist(shouldStopTask)){
                        LOGGER.info("start to clean up "+shouldStopTask);
                        LOGGER.info("clean up scheduler");
                        GetRecentFinishedFileListTask stopTask = gson.fromJson(configService.get(shouldStopTask),GetRecentFinishedFileListTask.class);
                        stopTask.cleanup();
                        LOGGER.info("clean up schedule successfully");
                        LOGGER.info("start to clean up configEnity");
                        configService.delete(shouldStopTask);
                        LOGGER.info("clean up config successfully");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
