package com.weibo.dip.data.platform.falcon.scheduler.task;

import com.weib.dip.data.platform.services.client.model.HFileStatus;
import com.weibo.dip.data.platform.falcon.scheduler.model.Task;
import com.weibo.dip.data.platform.falcon.scheduler.util.ConfigUtils;
import com.weibo.dip.data.platform.falcon.scheduler.util.HdfsUtils;

import java.util.Date;
import java.util.List;

/**
 * Created by Wen on 2017/2/16.
 *
 */
public class HdfsGetTask extends Task {

    private long startTime;

    private long endTime;

    private String category;

    public HdfsGetTask(String category,long startTime,long endTime) {
        this.category = category;
        this.startTime = startTime;
        this.endTime = endTime;
//        setScheduleTime(new Date());
    }


    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    @Override
    public void setup() throws Exception {
//        startTime = getScheduleTime().getTime() - ConfigUtils.TIME_HDFS_INTERVAL;
//        endTime = getScheduleTime().getTime();
    }

    @Override
    public void execute() throws Exception {
        List<HFileStatus> list = HdfsUtils.getRecentFileListByHdfs(category,startTime,endTime);
        //write to someplace
    }

    @Override
    public void cleanup() throws Exception {

    }

    @Override
    public int compareTo(Task o) {
        return 0;
    }
}
