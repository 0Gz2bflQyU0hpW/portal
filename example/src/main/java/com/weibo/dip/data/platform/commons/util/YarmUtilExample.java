package com.weibo.dip.data.platform.commons.util;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import java.util.List;

/**
 * Created by yurun on 17/11/3.
 */
public class YarmUtilExample {

    public static void main(String[] args) throws Exception {
        List<ApplicationReport> allApplications = YarnUtil.getApplications();

        System.out.println("all: " + allApplications.size());

        List<ApplicationReport> runningApplications = YarnUtil.getApplications(YarnApplicationState.RUNNING);

        System.out.println("running: " + runningApplications.size());

        String appName = "select_job_sinaedge_trafficserver_minf_ts_2017110311-hour_rawlog";

        ApplicationReport applicationReport = YarnUtil.getApplication(appName);

        System.out.println(applicationReport.getApplicationId().toString());

        ApplicationReport applicationReport1 = YarnUtil.getApplication(appName, YarnApplicationState.RUNNING);

        System.out.println(applicationReport.getApplicationType());
    }

}
