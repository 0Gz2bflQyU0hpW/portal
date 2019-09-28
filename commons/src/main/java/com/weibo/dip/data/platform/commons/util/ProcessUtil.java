package com.weibo.dip.data.platform.commons.util;

import java.lang.management.ManagementFactory;

/**
 * Created by yurun on 17/10/23.
 */
public class ProcessUtil {

    public static String getPid() {
        return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
    }

    public static void main(String[] args) {
        System.out.println(ProcessUtil.getPid());
    }

}
