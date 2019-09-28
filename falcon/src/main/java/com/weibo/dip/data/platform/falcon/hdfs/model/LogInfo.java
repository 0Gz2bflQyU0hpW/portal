package com.weibo.dip.data.platform.falcon.hdfs.model;

import java.util.Map;

/**
 * Created by Wen on 2016/12/22.
 *
 */
public class LogInfo {
    private String logName;
    private Map<String,LogDailySize> dailyMap;
    private String owner;
    private String group;
    public String getLogName() {
        return logName;
    }

    public void setLogName(String logName) {
        this.logName = logName;
    }

    public Map<String, LogDailySize> getDailyMap() {
        return dailyMap;
    }

    public void setDailyMap(Map<String, LogDailySize> dailyMap) {
        this.dailyMap = dailyMap;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }
}
