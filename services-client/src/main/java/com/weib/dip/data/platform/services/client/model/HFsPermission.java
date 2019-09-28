package com.weib.dip.data.platform.services.client.model;

import java.io.Serializable;

/**
 * Created by yurun on 16/12/20.
 */
public class HFsPermission implements Serializable {

    private String user;
    private String group;
    private String other;

    public HFsPermission() {

    }

    public HFsPermission(String user, String group, String other) {
        this.user = user;
        this.group = group;
        this.other = other;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getOther() {
        return other;
    }

    public void setOther(String other) {
        this.other = other;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HFsPermission that = (HFsPermission) o;

        return (user != null ? user.equals(that.user) : that.user == null)
            && (group != null ? group.equals(that.group) : that.group == null)
            && (other != null ? other.equals(that.other) : that.other == null);
    }

    @Override
    public int hashCode() {
        int result = user != null ? user.hashCode() : 0;
        result = 31 * result + (group != null ? group.hashCode() : 0);
        result = 31 * result + (other != null ? other.hashCode() : 0);

        return result;
    }

    @Override
    public String toString() {
        return "HFsPermission{" +
            "user='" + user + '\'' +
            ", group='" + group + '\'' +
            ", other='" + other + '\'' +
            '}';
    }

}
