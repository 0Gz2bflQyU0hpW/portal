package com.weib.dip.data.platform.services.client.model;

import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * Created by ftx on 2016/12/22.
 */
public class HApplicationResourceUsageReport implements Serializable {
    private int numUsedContainers;
    private int numReservedContainers;
    private HResource usedResources;
    private HResource reservedResources;
    private HResource neededResources;
    private long memorySeconds;
    private long vcoreSeconds;

    public HApplicationResourceUsageReport() {
    }

    public HApplicationResourceUsageReport(int numUsedContainers, int numReservedContainers, HResource usedResources, HResource reservedResources, HResource neededResources, long memorySeconds, long vcoreSeconds) {
        this.numUsedContainers = numUsedContainers;
        this.numReservedContainers = numReservedContainers;
        this.usedResources = usedResources;
        this.reservedResources = reservedResources;
        this.neededResources = neededResources;
        this.memorySeconds = memorySeconds;
        this.vcoreSeconds = vcoreSeconds;
    }

    public int getNumUsedContainers() {
        return numUsedContainers;
    }

    public void setNumUsedContainers(int numUsedContainers) {
        this.numUsedContainers = numUsedContainers;
    }

    public long getVcoreSeconds() {
        return vcoreSeconds;
    }

    public void setVcoreSeconds(long vcoreSeconds) {
        this.vcoreSeconds = vcoreSeconds;
    }

    public long getMemorySeconds() {
        return memorySeconds;
    }

    public void setMemorySeconds(long memorySeconds) {
        this.memorySeconds = memorySeconds;
    }

    public HResource getNeededResources() {
        return neededResources;
    }

    public void setNeededResources(HResource neededResources) {
        this.neededResources = neededResources;
    }

    public HResource getReservedResources() {
        return reservedResources;
    }

    public void setReservedResources(HResource reservedResources) {
        this.reservedResources = reservedResources;
    }

    public HResource getUsedResources() {
        return usedResources;
    }

    public void setUsedResources(HResource usedResources) {
        this.usedResources = usedResources;
    }

    public int getNumReservedContainers() {
        return numReservedContainers;
    }

    public void setNumReservedContainers(int numReservedContainers) {
        this.numReservedContainers = numReservedContainers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HApplicationResourceUsageReport that = (HApplicationResourceUsageReport) o;
        return Objects.equal(numUsedContainers, that.numUsedContainers) &&
                Objects.equal(numReservedContainers, that.numReservedContainers) &&
                Objects.equal(memorySeconds, that.memorySeconds) &&
                Objects.equal(vcoreSeconds, that.vcoreSeconds) &&
                Objects.equal(usedResources, that.usedResources) &&
                Objects.equal(reservedResources, that.reservedResources) &&
                Objects.equal(neededResources, that.neededResources);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(numUsedContainers, numReservedContainers, usedResources, reservedResources, neededResources, memorySeconds, vcoreSeconds);
    }

    @Override
    public String toString() {
        return "HApplicationResourceUsageReport{" +
                "numUsedContainers=" + numUsedContainers +
                ", numReservedContainers=" + numReservedContainers +
                ", usedResources=" + usedResources +
                ", reservedResources=" + reservedResources +
                ", neededResources=" + neededResources +
                ", memorySeconds=" + memorySeconds +
                ", vcoreSeconds=" + vcoreSeconds +
                '}';
    }
}
