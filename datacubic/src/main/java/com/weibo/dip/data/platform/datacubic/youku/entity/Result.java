package com.weibo.dip.data.platform.datacubic.youku.entity;

/**
 * @author delia
 */

public class Result {

    private String vid;
    private String domainid;
    private Data data;

    public Result() {

    }

    public Result(String vid, String domainid) {
        this.vid = vid;
        this.domainid = domainid;
        this.data = new Data();
    }

    public String getVid() {
        return vid;
    }

    public void setVid(String vid) {
        this.vid = vid;
    }

    public String getDomainid() {
        return domainid;
    }

    public void setDomainid(String domainid) {
        this.domainid = domainid;
    }

    public Data getData() {
        return data;
    }

    public void setData(Data data) {
        this.data = data;
    }

    public void setData(String platform, int hour, long count) {
        if (platform.equals("android")) {
            data.getAndroid().getHour()[hour] = count;
        } else if (platform.equals("ios")) {
            data.getIos().getHour()[hour] = count;
        }
    }

}


