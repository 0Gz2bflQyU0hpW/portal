package com.weibo.dip.costing.bean;

/**
 * Created by yurun on 18/4/23.
 */
public class DipCost {

  private int id;
  private String year;
  private String month;
  private String productName;
  private String productUuid;
  private double costQuota;
  private String quotaUnit;
  private String costType;
  private double percent;
  private String descs;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getYear() {
    return year;
  }

  public void setYear(String year) {
    this.year = year;
  }

  public String getMonth() {
    return month;
  }

  public void setMonth(String month) {
    this.month = month;
  }

  public String getProductName() {
    return productName;
  }

  public void setProductName(String productName) {
    this.productName = productName;
  }

  public String getProductUuid() {
    return productUuid;
  }

  public void setProductUuid(String productUuid) {
    this.productUuid = productUuid;
  }

  public double getCostQuota() {
    return costQuota;
  }

  public void setCostQuota(double costQuota) {
    this.costQuota = costQuota;
  }

  public String getQuotaUnit() {
    return quotaUnit;
  }

  public void setQuotaUnit(String quotaUnit) {
    this.quotaUnit = quotaUnit;
  }

  public String getCostType() {
    return costType;
  }

  public void setCostType(String costType) {
    this.costType = costType;
  }

  public double getPercent() {
    return percent;
  }

  public void setPercent(double percent) {
    this.percent = percent;
  }

  public String getDescs() {
    return descs;
  }

  public void setDescs(String descs) {
    this.descs = descs;
  }

}
