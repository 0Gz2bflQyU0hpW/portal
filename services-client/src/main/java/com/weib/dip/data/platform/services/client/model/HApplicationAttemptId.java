package com.weib.dip.data.platform.services.client.model;

import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * Created by ftx on 2016/12/22.
 */
public class HApplicationAttemptId implements Serializable {
    private HApplicationId appId;
    private int attemptId;

    public HApplicationAttemptId() {
    }

    public HApplicationAttemptId(HApplicationId appId, int attemptId) {
        this.appId = appId;
        this.attemptId = attemptId;
    }

    public HApplicationId getAppId() {
        return appId;
    }

    public void setAppId(HApplicationId appId) {
        this.appId = appId;
    }

    public int getAttemptId() {
        return attemptId;
    }

    public void setAttemptId(int attemptId) {
        this.attemptId = attemptId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HApplicationAttemptId that = (HApplicationAttemptId) o;
        return Objects.equal(attemptId, that.attemptId) &&
                Objects.equal(appId, that.appId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(appId, attemptId);
    }

    @Override
    public String toString() {
        return "HApplicationAttemptId{" +
                "appId=" + appId +
                ", attemptId=" + attemptId +
                '}';
    }
}
