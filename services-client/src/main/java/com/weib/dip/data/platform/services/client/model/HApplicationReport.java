package com.weib.dip.data.platform.services.client.model;

import com.google.common.base.Objects;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import java.io.Serializable;

/**
 * Created by ftx on 2016/12/22.
 */
public class HApplicationReport implements Serializable{
    private HApplicationId applicationId;
    private HApplicationAttemptId HApplicationAttemptId;
    private String user;
    private String queue;
    private String name;
    private String host;
    private int rpcPort;
    private HToken hToken;
    private YarnApplicationState state;
    private String diagnostics;
    private String url;
    private long startTime;
    private long finishTime;
    private FinalApplicationStatus finalStatus;
    private HApplicationResourceUsageReport appResources;
    private String origTrackingUrl;
    private float progress;
    private String applicationType;
    private HToken amRmToken;

    public HApplicationReport() {
    }

    public HApplicationReport(HApplicationId applicationId, com.weib.dip.data.platform.services.client.model.HApplicationAttemptId HApplicationAttemptId, String user, String queue, String name, String host, int rpcPort, HToken hToken, YarnApplicationState state, String diagnostics, String url, long startTime, long finishTime, FinalApplicationStatus finalStatus, HApplicationResourceUsageReport appResources, String origTrackingUrl, float progress, String applicationType, HToken amRmToken) {
        this.applicationId = applicationId;
        this.HApplicationAttemptId = HApplicationAttemptId;
        this.user = user;
        this.queue = queue;
        this.name = name;
        this.host = host;
        this.rpcPort = rpcPort;
        this.hToken = hToken;
        this.state = state;
        this.diagnostics = diagnostics;
        this.url = url;
        this.startTime = startTime;
        this.finishTime = finishTime;
        this.finalStatus = finalStatus;
        this.appResources = appResources;
        this.origTrackingUrl = origTrackingUrl;
        this.progress = progress;
        this.applicationType = applicationType;
        this.amRmToken = amRmToken;
    }

    public HApplicationId getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(HApplicationId applicationId) {
        this.applicationId = applicationId;
    }

    public com.weib.dip.data.platform.services.client.model.HApplicationAttemptId getHApplicationAttemptId() {
        return HApplicationAttemptId;
    }

    public void setHApplicationAttemptId(com.weib.dip.data.platform.services.client.model.HApplicationAttemptId HApplicationAttemptId) {
        this.HApplicationAttemptId = HApplicationAttemptId;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public void setRpcPort(int rpcPort) {
        this.rpcPort = rpcPort;
    }

    public HToken gethToken() {
        return hToken;
    }

    public void sethToken(HToken hToken) {
        this.hToken = hToken;
    }

    public YarnApplicationState getState() {
        return state;
    }

    public void setState(YarnApplicationState state) {
        this.state = state;
    }

    public String getDiagnostics() {
        return diagnostics;
    }

    public void setDiagnostics(String diagnostics) {
        this.diagnostics = diagnostics;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public FinalApplicationStatus getFinalStatus() {
        return finalStatus;
    }

    public void setFinalStatus(FinalApplicationStatus finalStatus) {
        this.finalStatus = finalStatus;
    }

    public HApplicationResourceUsageReport getAppResources() {
        return appResources;
    }

    public void setAppResources(HApplicationResourceUsageReport appResources) {
        this.appResources = appResources;
    }

    public String getOrigTrackingUrl() {
        return origTrackingUrl;
    }

    public void setOrigTrackingUrl(String origTrackingUrl) {
        this.origTrackingUrl = origTrackingUrl;
    }

    public float getProgress() {
        return progress;
    }

    public void setProgress(float progress) {
        this.progress = progress;
    }

    public String getApplicationType() {
        return applicationType;
    }

    public void setApplicationType(String applicationType) {
        this.applicationType = applicationType;
    }

    public HToken getAmRmToken() {
        return amRmToken;
    }

    public void setAmRmToken(HToken amRmToken) {
        this.amRmToken = amRmToken;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HApplicationReport that = (HApplicationReport) o;
        return Objects.equal(rpcPort, that.rpcPort) &&
                Objects.equal(startTime, that.startTime) &&
                Objects.equal(finishTime, that.finishTime) &&
                Objects.equal(progress, that.progress) &&
                Objects.equal(applicationId, that.applicationId) &&
                Objects.equal(HApplicationAttemptId, that.HApplicationAttemptId) &&
                Objects.equal(user, that.user) &&
                Objects.equal(queue, that.queue) &&
                Objects.equal(name, that.name) &&
                Objects.equal(host, that.host) &&
                Objects.equal(hToken, that.hToken) &&
                Objects.equal(state, that.state) &&
                Objects.equal(diagnostics, that.diagnostics) &&
                Objects.equal(url, that.url) &&
                Objects.equal(finalStatus, that.finalStatus) &&
                Objects.equal(appResources, that.appResources) &&
                Objects.equal(origTrackingUrl, that.origTrackingUrl) &&
                Objects.equal(applicationType, that.applicationType) &&
                Objects.equal(amRmToken, that.amRmToken);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(applicationId, HApplicationAttemptId, user, queue, name, host, rpcPort, hToken, state, diagnostics, url, startTime, finishTime, finalStatus, appResources, origTrackingUrl, progress, applicationType, amRmToken);
    }

    @Override
    public String toString() {
        return "HApplicationReport{" +
                "applicationId=" + applicationId +
                ", HApplicationAttemptId=" + HApplicationAttemptId +
                ", user='" + user + '\'' +
                ", queue='" + queue + '\'' +
                ", name='" + name + '\'' +
                ", host='" + host + '\'' +
                ", rpcPort=" + rpcPort +
                ", hToken=" + hToken +
                ", state=" + state +
                ", diagnostics='" + diagnostics + '\'' +
                ", url='" + url + '\'' +
                ", startTime=" + startTime +
                ", finishTime=" + finishTime +
                ", finalStatus=" + finalStatus +
                ", appResources=" + appResources +
                ", origTrackingUrl='" + origTrackingUrl + '\'' +
                ", progress=" + progress +
                ", applicationType='" + applicationType + '\'' +
                ", amRmToken=" + amRmToken +
                '}';
    }
}
