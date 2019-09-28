package com.weibo.dip.data.platform.falcon.scheduler.service;

import com.google.gson.Gson;
import com.weib.dip.data.platform.services.client.ConfigService;

import com.weib.dip.data.platform.services.client.model.ConfigEntity;
import com.weib.dip.data.platform.services.client.model.HFileStatus;
import com.weib.dip.data.platform.services.client.util.ServiceProxyBuilder;
import com.weibo.dip.data.platform.falcon.scheduler.model.GetRecentFinishedFileListTask;
import com.weibo.dip.data.platform.falcon.hdfs.service.HdfsService;

import org.apache.commons.collections.CollectionUtils;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

/**
 * Created by Wen on 2017/1/18.
 *
 */
public class GetFileListByHDFS implements Job {
    private  final static Logger LOGGER = LoggerFactory.getLogger(GetFileListByHDFS.class);
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        //get from JobDataMap
        HdfsService hdfsService = new HdfsService();
        Gson gson = new Gson();
        List<HFileStatus> hFileStatusList;

        JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
        String dataSetName = jobDataMap.getString("dataSetName");
        String path = jobDataMap.getString("path");

        //setDB
        ConfigService configService = ServiceProxyBuilder.buildLocalhost(ConfigService.class);
        try {
            ConfigEntity configEntity = configService.getConfigEntity(dataSetName);
            GetRecentFinishedFileListTask task = gson.fromJson(configService.get(dataSetName), GetRecentFinishedFileListTask.class);
            if (task.isSuccess()){
                LOGGER.info("updating config");
                task.setScheduleTime(task.getEndTime());
                task.setSuccess(false);
                task.setEndTime(new Date(task.getScheduleTime().getTime()+task.getIntervalSeconds()*1000));
                configEntity.setValue(gson.toJson(task));
                configService.update(configEntity);
                LOGGER.info("updating successfully");
            }
            hFileStatusList = hdfsService.getRecentFinishedFiles(path,dataSetName,task.getScheduleTime().getTime(),task.getEndTime().getTime());
            if (!CollectionUtils.isEmpty(hFileStatusList)){
                //write
                LOGGER.info("writting in database");
                task.setSuccess(true);
                LOGGER.info("start writting dataSet ");
                //write is finished
                LOGGER.info("writting dataSet successfully");
                configEntity.setKey(dataSetName);
                configEntity.setValue(gson.toJson(task));
                if (configService.exist(dataSetName)){
                    configService.update(configEntity);
                }
                else{
                    configService.insert(configEntity);
                }
                LOGGER.info("writting config successfully");

            }
            else{
                LOGGER.error("Something failed");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
