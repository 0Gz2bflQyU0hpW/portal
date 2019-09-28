package com.weibo.dip.data.platform.falcon.scheduler.test;

/**
 * Created by Wen on 2017/1/19.
 */
public class User {
    String name;
    int age;
    String address;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public User(String name, int age, String address) {
        this.name = name;
        this.age = age;
        this.address = address;
    }

    public User(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public User() {
        this.name = "";
        this.age = 0;
        this.address="";
    }

}
