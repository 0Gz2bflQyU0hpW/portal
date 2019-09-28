package com.weibo.dip.data.platform.services.server.impl;

import com.weib.dip.data.platform.services.client.YarnService;
import com.weib.dip.data.platform.services.client.model.*;
import com.weibo.dip.data.platform.commons.util.HadoopConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

/**
 * Created by yurun on 16/12/14.
 */
@Service
public class YarnServiceImpl implements YarnService {

    private static final Logger LOGGER = LoggerFactory.getLogger(YarnServiceImpl.class);

    private static final YarnClient YARN_CLIENT = YarnClient.createYarnClient();

    static {
        YARN_CLIENT.init(HadoopConfiguration.getInstance());
        YARN_CLIENT.start();
    }


    @Override
    public YarnApplicationState newYarnApplicationStateRunning() {
        return YarnApplicationState.RUNNING;
    }

    @Override
    public boolean isRunning(HApplicationId appId) throws Exception {
        return true;
    }

    @Override
    public boolean isFinished(HApplicationId appId) throws Exception {
        return false;
    }

    @Override
    public List<HApplicationReport> getApplications() throws Exception {
        return changeApplicationReportToHApplicationReport(YARN_CLIENT.getApplications());
    }

    @Override
    public List<HApplicationReport> getApplications(YarnApplicationState applicationState) throws Exception {
        return changeApplicationReportToHApplicationReport(YARN_CLIENT.getApplications(EnumSet.of(applicationState)));
    }

    @Override
    public List<HApplicationReport> getApplications(Set<String> applicationTypes) throws Exception {
        return changeApplicationReportToHApplicationReport(YARN_CLIENT.getApplications(applicationTypes));
    }

    @Override
    public List<HApplicationReport> getApplications(Set<String> applicationTypes, EnumSet<YarnApplicationState> applicationStates) throws Exception {
        return changeApplicationReportToHApplicationReport(YARN_CLIENT.getApplications(applicationTypes, applicationStates));
    }

    @Override
    public void killApplication(HApplicationId applicationId) throws Exception {
        ApplicationId appId = ApplicationId.newInstance(applicationId.getClusterTimestamp(), applicationId.getId());
            YARN_CLIENT.killApplication(appId);
        LOGGER.info("kill Application appId= " + appId);
    }

    @Override
    public List<HApplicationReport> changeApplicationReportToHApplicationReport(List<ApplicationReport> applicationReports) throws Exception {
        List<HApplicationReport> hApplicationReports = new ArrayList<HApplicationReport>();
        for (ApplicationReport applicationReport : applicationReports) {
            HApplicationReport hApplicationReport = new HApplicationReport();
            HApplicationId hApplicationId = new HApplicationId();
            hApplicationId.setClusterTimestamp(applicationReport.getApplicationId().getClusterTimestamp());
            hApplicationId.setId(applicationReport.getApplicationId().getId());
            hApplicationReport.setApplicationId(hApplicationId);

            HApplicationAttemptId hApplicationAttemptId = new HApplicationAttemptId();
            hApplicationAttemptId.setAppId(hApplicationId);
            hApplicationAttemptId.setAttemptId(applicationReport.getCurrentApplicationAttemptId().getAttemptId());
            hApplicationReport.setHApplicationAttemptId(hApplicationAttemptId);

            hApplicationReport.setUser(applicationReport.getUser());

            hApplicationReport.setQueue(applicationReport.getQueue());

            hApplicationReport.setName(applicationReport.getName());

            hApplicationReport.setHost(applicationReport.getHost());

            hApplicationReport.setRpcPort(applicationReport.getRpcPort());

            HToken hToken = new HToken();
            if (applicationReport.getClientToAMToken() != null) {
                hToken.setIdentifier(applicationReport.getClientToAMToken().getIdentifier().array());
                hToken.setKind(applicationReport.getClientToAMToken().getKind());
                hToken.setPassword(applicationReport.getClientToAMToken().getPassword().array());
                hToken.setService(applicationReport.getClientToAMToken().getService());
            }
            hApplicationReport.sethToken(hToken);

            hApplicationReport.setState(applicationReport.getYarnApplicationState());

            hApplicationReport.setDiagnostics(applicationReport.getDiagnostics());

            hApplicationReport.setUrl(applicationReport.getTrackingUrl());

            hApplicationReport.setStartTime(applicationReport.getStartTime());

            hApplicationReport.setFinishTime(applicationReport.getFinishTime());

            hApplicationReport.setFinalStatus(applicationReport.getFinalApplicationStatus());

            HApplicationResourceUsageReport appResources = new HApplicationResourceUsageReport();
            appResources.setNumUsedContainers(applicationReport.getApplicationResourceUsageReport().getNumUsedContainers());
            appResources.setNumReservedContainers(applicationReport.getApplicationResourceUsageReport().getNumReservedContainers());
            appResources.setMemorySeconds(applicationReport.getApplicationResourceUsageReport().getMemorySeconds());
            appResources.setVcoreSeconds(applicationReport.getApplicationResourceUsageReport().getVcoreSeconds());
            HResource usedResources = new HResource();
            usedResources.setMemory(applicationReport.getApplicationResourceUsageReport().getUsedResources().getMemory());
            usedResources.setvCores(applicationReport.getApplicationResourceUsageReport().getUsedResources().getVirtualCores());
            appResources.setUsedResources(usedResources);
            HResource reservedResources = new HResource();
            reservedResources.setMemory(applicationReport.getApplicationResourceUsageReport().getReservedResources().getMemory());
            reservedResources.setvCores(applicationReport.getApplicationResourceUsageReport().getReservedResources().getVirtualCores());
            appResources.setReservedResources(reservedResources);
            HResource neededResources = new HResource();
            neededResources.setMemory(applicationReport.getApplicationResourceUsageReport().getNeededResources().getMemory());
            neededResources.setvCores(applicationReport.getApplicationResourceUsageReport().getNeededResources().getVirtualCores());
            appResources.setNeededResources(neededResources);
            hApplicationReport.setAppResources(appResources);

            hApplicationReport.setOrigTrackingUrl(applicationReport.getOriginalTrackingUrl());

            hApplicationReport.setProgress(applicationReport.getProgress());

            hApplicationReport.setApplicationType(applicationReport.getApplicationType());

            HToken amRmToken = new HToken();
            if (applicationReport.getAMRMToken() != null) {
                amRmToken.setIdentifier(applicationReport.getAMRMToken().getIdentifier().array());
                amRmToken.setKind(applicationReport.getAMRMToken().getKind());
                amRmToken.setPassword(applicationReport.getAMRMToken().getPassword().array());
                amRmToken.setService(applicationReport.getAMRMToken().getService());
            }
            hApplicationReport.setAmRmToken(amRmToken);

            hApplicationReports.add(hApplicationReport);
        }

        return hApplicationReports;
    }

}
