package com.weibo.dip.data.platform.commons.util;

import org.apache.hadoop.yarn.api.records.ApplicationReport;

import java.util.List;

/**
 * Created by yurun on 17/11/3.
 */
public class YarnUtilMain {

    public static void main(String[] args) throws Exception {
        List<ApplicationReport> applicationReports = YarnUtil.getApplications();
    }

}
