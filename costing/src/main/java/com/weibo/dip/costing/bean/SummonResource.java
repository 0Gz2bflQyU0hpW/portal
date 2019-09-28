package com.weibo.dip.costing.bean;

import java.util.Date;

/**
 * Created by yurun on 18/4/25.
 */
public class SummonResource {

  private int id;
  private String businessName;
  private String productUuid;
  private int disk;
  private int indexQps;
  private Date collectedTime;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getBusinessName() {
    return businessName;
  }

  public void setBusinessName(String businessName) {
    this.businessName = businessName;
  }

  public String getProductUuid() {
    return productUuid;
  }

  public void setProductUuid(String productUuid) {
    this.productUuid = productUuid;
  }

  public int getDisk() {
    return disk;
  }

  public void setDisk(int disk) {
    this.disk = disk;
  }

  public int getIndexQps() {
    return indexQps;
  }

  public void setIndexQps(int indexQps) {
    this.indexQps = indexQps;
  }

  public Date getCollectedTime() {
    return collectedTime;
  }

  public void setCollectedTime(Date collectedTime) {
    this.collectedTime = collectedTime;
  }

}
