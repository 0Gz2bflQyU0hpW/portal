package com.weib.dip.data.platform.services.client.model;

import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * Created by ftx on 2016/12/22.
 */
public class HResource implements Serializable{
    private int memory;
    private int vCores;

    public HResource() {
    }

    public HResource(int memory, int vCores) {
        this.memory = memory;
        this.vCores = vCores;
    }

    public int getMemory() {
        return memory;
    }

    public void setMemory(int memory) {
        this.memory = memory;
    }

    public int getvCores() {
        return vCores;
    }

    public void setvCores(int vCores) {
        this.vCores = vCores;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HResource hResource = (HResource) o;
        return Objects.equal(memory, hResource.memory) &&
                Objects.equal(vCores, hResource.vCores);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(memory, vCores);
    }

    @Override
    public String toString() {
        return "HResource{" +
                "memory=" + memory +
                ", vCores=" + vCores +
                '}';
    }
}
