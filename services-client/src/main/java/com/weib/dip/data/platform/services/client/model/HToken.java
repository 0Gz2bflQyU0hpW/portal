package com.weib.dip.data.platform.services.client.model;

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by ftx on 2016/12/22.
 */
public class HToken implements Serializable{
    private byte[] identifier;
    private String kind;
    private byte[] password;
    private String service;

    public HToken() {
    }

    public HToken(byte[] identifier, String kind, byte[] password, String service) {
        this.identifier = identifier;
        this.kind = kind;
        this.password = password;
        this.service = service;
    }

    public byte[] getIdentifier() {
        return identifier;
    }

    public void setIdentifier(byte[] identifier) {
        this.identifier = identifier;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public byte[] getPassword() {
        return password;
    }

    public void setPassword(byte[] password) {
        this.password = password;
    }

    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HToken hToken = (HToken) o;
        return Objects.equal(identifier, hToken.identifier) &&
                Objects.equal(kind, hToken.kind) &&
                Objects.equal(password, hToken.password) &&
                Objects.equal(service, hToken.service);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(identifier, kind, password, service);
    }

    @Override
    public String toString() {
        return "HToken{" +
                "identifier=" + Arrays.toString(identifier) +
                ", kind='" + kind + '\'' +
                ", password=" + Arrays.toString(password) +
                ", service='" + service + '\'' +
                '}';
    }
}
