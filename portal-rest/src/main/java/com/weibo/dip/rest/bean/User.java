package com.weibo.dip.rest.bean;

import java.util.Date;

/**
 * Created by yurun on 17/10/20.
 */
public class User {

    private String userId;
    private String userName;
    private String password;
    private String role;
    private String trueName;
    private String sex;
    private String dept;
    private String position;
    private String mobile;
    private String email;
    private String noticeType;
    private String noticeMethod;
    private String noticeEmail;
    private String noticeMobile;
    private String accesskey;
    private int departlevel;
    private Date registrationTime;

    public User() {

    }

    public User(String userId, String userName, String password, String role, String trueName, String sex,
                String dept, String position, String mobile, String email, String noticeType, String
                    noticeMethod, String noticeEmail, String noticeMobile, String accesskey, int
                    departlevel, Date registrationTime) {
        this.userId = userId;
        this.userName = userName;
        this.password = password;
        this.role = role;
        this.trueName = trueName;
        this.sex = sex;
        this.dept = dept;
        this.position = position;
        this.mobile = mobile;
        this.email = email;
        this.noticeType = noticeType;
        this.noticeMethod = noticeMethod;
        this.noticeEmail = noticeEmail;
        this.noticeMobile = noticeMobile;
        this.accesskey = accesskey;
        this.departlevel = departlevel;
        this.registrationTime = registrationTime;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getTrueName() {
        return trueName;
    }

    public void setTrueName(String trueName) {
        this.trueName = trueName;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getDept() {
        return dept;
    }

    public void setDept(String dept) {
        this.dept = dept;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getNoticeType() {
        return noticeType;
    }

    public void setNoticeType(String noticeType) {
        this.noticeType = noticeType;
    }

    public String getNoticeMethod() {
        return noticeMethod;
    }

    public void setNoticeMethod(String noticeMethod) {
        this.noticeMethod = noticeMethod;
    }

    public String getNoticeEmail() {
        return noticeEmail;
    }

    public void setNoticeEmail(String noticeEmail) {
        this.noticeEmail = noticeEmail;
    }

    public String getNoticeMobile() {
        return noticeMobile;
    }

    public void setNoticeMobile(String noticeMobile) {
        this.noticeMobile = noticeMobile;
    }

    public String getAccesskey() {
        return accesskey;
    }

    public void setAccesskey(String accesskey) {
        this.accesskey = accesskey;
    }

    public int getDepartlevel() {
        return departlevel;
    }

    public void setDepartlevel(int departlevel) {
        this.departlevel = departlevel;
    }

    public Date getRegistrationTime() {
        return registrationTime;
    }

    public void setRegistrationTime(Date registrationTime) {
        this.registrationTime = registrationTime;
    }

}
