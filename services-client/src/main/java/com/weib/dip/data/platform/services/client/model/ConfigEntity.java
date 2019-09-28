package com.weib.dip.data.platform.services.client.model;

import java.io.Serializable;
import java.util.Date;

/**
 * @author delia
 */
public class ConfigEntity implements Serializable {

    public static final String DEFAULT_USER = "default";
    public static final String DEFAULT_COMMENT = "";

    private String user = DEFAULT_USER;
    private String key = null;
    private String value = null;
    private boolean isCached = true;
    private String comment = "";
    private Date updateTime = new Date();

    public ConfigEntity() {
    }

    public ConfigEntity(String key, String value) {
        this(DEFAULT_USER, key, value);
    }

    public ConfigEntity(String key, String value, boolean isCached) {
        this(key, value, isCached, DEFAULT_COMMENT);
    }

    public ConfigEntity(String key, String value, boolean isCached, String comment) {
        this(DEFAULT_USER, key, value, isCached, comment);
    }

    public ConfigEntity(String key, String value, boolean isCached, String comment, Date updateTime) {
        this(DEFAULT_USER, key, value, isCached, comment, updateTime);
    }

    public ConfigEntity(String user, String key, String value) {
        this(user, key, value, true, DEFAULT_COMMENT);
    }

    public ConfigEntity(String user, String key, String value, boolean isCached) {
        this(user, key, value, isCached, DEFAULT_COMMENT);
    }

    public ConfigEntity(String user, String key, String value, boolean isCached, String comment) {
        this(user, key, value, isCached, comment, new Date());
    }

    public ConfigEntity(String user, String key, String value, boolean isCached, String comment, Date updateTime) {
        this.user = user;
        this.key = key;
        this.value = value;
        this.isCached = isCached;
        this.comment = comment;
        this.updateTime = updateTime;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public boolean isCached() {
        return isCached;
    }

    public void setCached(boolean cached) {
        this.isCached = cached;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConfigEntity that = (ConfigEntity) o;

        return (user != null ? user.equals(that.user) : that.user == null)
            && (key != null ? key.equals(that.key) : that.key == null)
            && (value != null ? value.equals(that.value) : that.value == null)
            && (isCached == that.isCached)
            && (comment != null ? comment.equals(that.comment) : that.comment == null)
            && (updateTime != null ? updateTime.equals(that.updateTime) : that.updateTime == null);
    }

    @Override
    public int hashCode() {
        int result = user != null ? user.hashCode() : 0;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (isCached ? 1 : 0);
        result = 31 * result + (comment != null ? comment.hashCode() : 0);
        result = 31 * result + (updateTime != null ? updateTime.hashCode() : 0);

        return result;
    }

    @Override
    public String toString() {
        return "ConfigEntity{" +
            "user='" + user + '\'' +
            ", key='" + key + '\'' +
            ", value='" + value + '\'' +
            ", isCached=" + isCached +
            ", comment='" + comment + '\'' +
            ", updateTime='" + updateTime + '\'' +
            '}';
    }

}
