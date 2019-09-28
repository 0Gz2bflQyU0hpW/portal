package com.weibo.dip.data.platform.falcon.scheduler.model;

import com.google.gson.Gson;
import com.weib.dip.data.platform.services.client.ConfigService;
import com.weib.dip.data.platform.services.client.model.ConfigEntity;
import com.weib.dip.data.platform.services.client.util.ServiceProxyBuilder;
import com.weibo.dip.data.platform.falcon.scheduler.service.GetDataSetList;
import com.weibo.dip.data.platform.falcon.scheduler.service.GetFileListByDB;
import com.weibo.dip.data.platform.falcon.scheduler.service.GetFileListByHDFS;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by Wen on 2017/1/18.
 *
 */
public class Worker {
    private final static Logger LOGGER = LoggerFactory.getLogger(Worker.class);
    private ConfigService configService = ServiceProxyBuilder.buildLocalhost(ConfigService.class);

    public List<String> getDataSetList() throws Exception {

        LOGGER.info("getting the DataSetList");
        List<String> dataSetList = new ArrayList<>();
        //addSome
        LOGGER.info("getting the DataSetList ok");

        return dataSetList;
    }

    public void updateTaskSchedule() throws Exception {
        GetRecentFinishedFileListTask updateTask = new GetRecentFinishedFileListTask();
        updateTask.setClassName(GetDataSetList.class);
        updateTask.setScheduleTime(new Date());
        updateTask.setTriggerName("UpdateDataSetList-Tirgger");
        updateTask.setJobName("UpdateDataSetList-Job");
        updateTask.setIntervalSeconds(1800);  //update strategy time
        updateTask.setup();
        updateTask.execute();
    }


    public void startGetRecentFinishedFileListTask(List<String> dataSetList,int strategy) throws Exception {

//        List<String> list = getDataSetList(); //获得要执行的数据集

        Gson gson = new Gson();
        ConfigEntity dataSetListEnity = new ConfigEntity();
        LOGGER.info("writting config");

        dataSetListEnity.setKey("DataSetList");
        dataSetListEnity.setValue(StringUtils.join(dataSetList,","));

        if (configService.exist("DataSetList")){
            configService.update(dataSetListEnity);
        }
        else{
            configService.insert(dataSetListEnity);
        }
        LOGGER.info("writting config successfully");

        for (String dataSetName : dataSetList) {
            GetRecentFinishedFileListTask task = new GetRecentFinishedFileListTask();
            if (configService.exist(dataSetName)) {
                //isFinished == true 上次任务完成从下次开始
                if (task.isSuccess()){
                    LOGGER.info("dataSetName exists,may recover");
                    task = gson.fromJson(configService.get("dataSetName"), GetRecentFinishedFileListTask.class);
                    task.setScheduleTime(task.getEndTime());
                    task.setEndTime(new Date());
                }
                //上次任务没有完成
                else{
                    LOGGER.info("dataSetName exists,may recover");
                    task = gson.fromJson(configService.get("dataSetName"), GetRecentFinishedFileListTask.class);
                    task.setScheduleTime(task.getScheduleTime());
                    task.setEndTime(task.getEndTime());
                }
            } 
            else {
                if (task.getJobType().equals("DB"))
                    task.setClassName(GetFileListByDB.class);
                else
                    task.setClassName(GetFileListByHDFS.class);
                task.setJobName(dataSetName+"-Job");
                task.setTriggerName(dataSetName+"-trigger");
                task.setDataSetName(dataSetName);
                task.setScheduleTime(new Date(new Date().getTime()-strategy*1000));
                task.setEndTime(new Date());
                task.setIntervalSeconds(strategy);
                task.createDataMap();
                ConfigEntity configEntity = new ConfigEntity();
                configEntity.setKey(dataSetName);
                configEntity.setValue(gson.toJson(task));
                configService.insert(configEntity);
            }

            task.setup();
            // insert executeTime,isFinished
            task.execute();
        }
    }
}
