package com.weibo.dip.portal.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author : lihx create date : 2014-12-2
 */
public class CuratorManager {
    private static transient Logger log = LoggerFactory.getLogger(CuratorManager.class);
    private static CuratorFramework curator;

    public static void createCuratorFramework(String zkStr) throws Exception {
        int connectionTimeoutMs = 10000;
        int sessionTimeoutMs = 30 * 1000;
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);

        curator = CuratorFrameworkFactory.builder().connectString(zkStr).retryPolicy(retryPolicy)
                .connectionTimeoutMs(connectionTimeoutMs)
                .sessionTimeoutMs(sessionTimeoutMs)
                .build();
        curator.start();

    }

    public static CuratorFramework getCurator(String zkStr) throws Exception {
        if (curator == null) {
            createCuratorFramework(zkStr);
        }
        return curator;
    }
}
