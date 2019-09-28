package com.weibo.dip.data.platform.datacubic.druid.query;

import java.util.Date;

/**
 * Created by yurun on 17/1/23.
 */
public class Interval {

    private Date startTime;

    private Date endTime;

    public Interval(Date startTime, Date endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

}
