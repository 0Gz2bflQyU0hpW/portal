package com.weib.dip.data.platform.services.client.model;

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by ftx on 2016/12/28.
 */
public class Dataset implements Serializable{
    private int id;
    private String name;
    private String product;
    private String department;
    private String type;
    private long size;
    private long saveTime;//时间间隔。单位秒
    private String[] contacts;
    private List<SaveStrategy> saveStrategy;
    private String comment;
    private Date createTtime;
    private Date updateTime;

    public Dataset() {
    }

    public Dataset(int id, String name, String product, String department, String type, long size, long saveTime, String[] contacts, List<SaveStrategy> saveStrategy, String comment, Date createTtime, Date updateTime) {
        this.id = id;
        this.name = name;
        this.product = product;
        this.department = department;
        this.type = type;
        this.size = size;
        this.saveTime = saveTime;
        this.contacts = contacts;
        this.saveStrategy = saveStrategy;
        this.comment = comment;
        this.createTtime = createTtime;
        this.updateTime = updateTime;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getSaveTime() {
        return saveTime;
    }

    public void setSaveTime(long saveTime) {
        this.saveTime = saveTime;
    }

    public String[] getContacts() {
        return contacts;
    }

    public void setContacts(String[] contacts) {
        this.contacts = contacts;
    }

    public List<SaveStrategy> getSaveStrategy() {
        return saveStrategy;
    }

    public void setSaveStrategy(List<SaveStrategy> saveStrategy) {
        this.saveStrategy = saveStrategy;
    }
    public String getSaveStrategyString() {
        String result = "";
        for (int index = 0; index < saveStrategy.size(); index++) {
            SaveStrategy saveStrategytemp = saveStrategy.get(index);
            result += saveStrategytemp.getStart() + "," + saveStrategytemp.getEnd() + "," + saveStrategytemp.getReplication() + ";";
        }
        if (result != null)
            result.substring(0, result.length() - 1);//去掉result字符串中的最后一个";"
        return result;
    }
    /**
     * 获取数据库str,封装成KeepStrategy
     *
     * @param str
     */
    public void setSaveStrategy(String str) {
        List<SaveStrategy> list = new ArrayList<>();
        String[] strategies = str.split(";");
        for (int index = 0; index < strategies.length; index++) {
            SaveStrategy saveStrategy = new SaveStrategy();
            String[] start_end_replication = strategies[index].split(",");
            saveStrategy.setStart(Long.parseLong(start_end_replication[0]));
            saveStrategy.setEnd(Long.parseLong(start_end_replication[1]));
            saveStrategy.setReplication(Integer.parseInt(start_end_replication[2]));
            list.add(saveStrategy);
        }
        this.setSaveStrategy(list);
    }
    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Date getCreateTtime() {
        return createTtime;
    }

    public void setCreateTtime(Date createTtime) {
        this.createTtime = createTtime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Dataset dataset = (Dataset) o;
        return Objects.equal(id, dataset.id) &&
                Objects.equal(size, dataset.size) &&
                Objects.equal(saveTime, dataset.saveTime) &&
                Objects.equal(name, dataset.name) &&
                Objects.equal(product, dataset.product) &&
                Objects.equal(department, dataset.department) &&
                Objects.equal(type, dataset.type) &&
                Objects.equal(contacts, dataset.contacts) &&
                Objects.equal(saveStrategy, dataset.saveStrategy) &&
                Objects.equal(comment, dataset.comment) &&
                Objects.equal(createTtime, dataset.createTtime) &&
                Objects.equal(updateTime, dataset.updateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, name, product, department, type, size, saveTime, contacts, saveStrategy, comment, createTtime, updateTime);
    }

    @Override
    public String toString() {
        return "Dataset{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", product='" + product + '\'' +
                ", department='" + department + '\'' +
                ", type='" + type + '\'' +
                ", size=" + size +
                ", saveTime=" + saveTime +
                ", contacts=" + Arrays.toString(contacts) +
                ", saveStrategy=" + saveStrategy +
                ", comment='" + comment + '\'' +
                ", createTtime=" + createTtime +
                ", updateTime=" + updateTime +
                '}';
    }
}
