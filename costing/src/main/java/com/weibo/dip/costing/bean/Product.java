package com.weibo.dip.costing.bean;

import java.util.Date;

/**
 * Created by yurun on 18/4/18.
 */
public class Product {

  private int id;
  private String uuid;
  private String name;
  private String techLeader;
  private String productLeader;
  private String vicePresident;
  private String status;
  private boolean isPlatform;
  private Date lastUpdate;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getTechLeader() {
    return techLeader;
  }

  public void setTechLeader(String techLeader) {
    this.techLeader = techLeader;
  }

  public String getProductLeader() {
    return productLeader;
  }

  public void setProductLeader(String productLeader) {
    this.productLeader = productLeader;
  }

  public String getVicePresident() {
    return vicePresident;
  }

  public void setVicePresident(String vicePresident) {
    this.vicePresident = vicePresident;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public boolean isPlatform() {
    return isPlatform;
  }

  public void setPlatform(boolean platform) {
    isPlatform = platform;
  }

  public Date getLastUpdate() {
    return lastUpdate;
  }

  public void setLastUpdate(Date lastUpdate) {
    this.lastUpdate = lastUpdate;
  }

}
