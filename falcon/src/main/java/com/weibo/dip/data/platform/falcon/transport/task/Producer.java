package com.weibo.dip.data.platform.falcon.transport.task;

/**
 * Created by Wen on 2017/2/19.
 */
public abstract class Producer {

    public abstract void start() throws Exception;

    public abstract void stop() throws Exception;
}
