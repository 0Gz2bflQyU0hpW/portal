package com.weib.dip.data.platform.services.client;

import com.weib.dip.data.platform.services.client.model.HApplicationId;
import com.weib.dip.data.platform.services.client.model.HApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Created by yurun on 16/12/14.
 */
public interface YarnService {

    YarnApplicationState newYarnApplicationStateRunning();

    boolean isRunning(HApplicationId appId) throws Exception;

    boolean isFinished(HApplicationId appId) throws Exception;

    List<HApplicationReport> getApplications() throws Exception;

    List<HApplicationReport> getApplications(YarnApplicationState applicationState) throws Exception;

    List<HApplicationReport> getApplications(Set<String> applicationTypes) throws Exception;

    List<HApplicationReport> getApplications(Set<String> applicationTypes, EnumSet<YarnApplicationState> applicationStates) throws Exception;

    void killApplication(HApplicationId applicationId) throws Exception;

    List<HApplicationReport> changeApplicationReportToHApplicationReport(List<ApplicationReport> applicationReports) throws Exception;
}