package com.weibo.dip.costing.bean;

import java.util.Date;

public class Dataset {

  private int id;
  private String name;
  private String productUuid;
  private int period;
  private double size;
  private String[] contacts;
  private String comment;
  private Date lastUpdate;

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

  public String getProductUuid() {
    return productUuid;
  }

  public void setProductUuid(String productUuid) {
    this.productUuid = productUuid;
  }

  public int getPeriod() {
    return period;
  }

  public void setPeriod(int period) {
    this.period = period;
  }

  public double getSize() {
    return size;
  }

  public void setSize(double size) {
    this.size = size;
  }

  public String[] getContacts() {
    return contacts;
  }

  public void setContacts(String[] contacts) {
    this.contacts = contacts;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public Date getLastUpdate() {
    return lastUpdate;
  }

  public void setLastUpdate(Date lastUpdate) {
    this.lastUpdate = lastUpdate;
  }
  
}
