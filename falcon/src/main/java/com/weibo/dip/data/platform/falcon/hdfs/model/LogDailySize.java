package com.weibo.dip.data.platform.falcon.hdfs.model;

import java.util.Map;

/**
 * Created by Wen on 2016/12/22.
 *
 */
public class LogDailySize {
    private String date;
    private Map<String,LogHourlySize> logSizeMap; //according to hour
    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Map<String, LogHourlySize> getLogSizeMap() {
        return logSizeMap;
    }

    public void setLogSizeMap(Map<String, LogHourlySize> logSizeMap) {
        this.logSizeMap = logSizeMap;
    }
}
