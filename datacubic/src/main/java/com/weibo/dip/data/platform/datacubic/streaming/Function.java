package com.weibo.dip.data.platform.datacubic.streaming;

import java.io.Serializable;

/**
 * Created by yurun on 17/1/17.
 */
public class Function implements Serializable {

    public static final String LOCAL = "local";

    private String name;

    private String classImpl;

    private String location;

    public Function() {

    }

    public Function(String name, String classImpl) {
        this.name = name;
        this.classImpl = classImpl;
        this.location = LOCAL;
    }

    public Function(String name, String classImpl, String location) {
        this.name = name;
        this.classImpl = classImpl;
        this.location = location;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClassImpl() {
        return classImpl;
    }

    public void setClassImpl(String classImpl) {
        this.classImpl = classImpl;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return "Function{" +
            "name='" + name + '\'' +
            ", classImpl='" + classImpl + '\'' +
            ", location='" + location + '\'' +
            '}';
    }

}
