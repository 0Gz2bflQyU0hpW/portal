package com.weibo.dip.data.platform.datacubic.youku.entity;

/**
 * @author delia
 */

public class Data {

    private Android android = new Android();
    private IOS ios = new IOS();

    public Data() {
        
    }

    public Android getAndroid() {
        return android;
    }

    public void setAndroid(Android android) {
        this.android = android;
    }

    public IOS getIos() {
        return ios;
    }

    public void setIos(IOS ios) {
        this.ios = ios;
    }

}

