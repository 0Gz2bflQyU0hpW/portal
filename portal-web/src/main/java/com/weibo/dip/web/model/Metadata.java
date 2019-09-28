package com.weibo.dip.web.model;

import java.util.List;

/**
 * Created by haisen on 2018/4/26.
 */
public class Metadata {

  //for test

  private int id;
  private String business;
  private List<String> dimensions;
  private List<String> metrics;

  /**
   * 构造函数.
   */
  public Metadata() {
  }

  public void setId(int id) {
    this.id = id;
  }

  public void setBusiness(String business) {
    this.business = business;
  }

  public void setDimension(List<String> dimensions) {
    this.dimensions = dimensions;
  }

  public void setMetrics(List<String> metric) {
    this.metrics = metric;
  }

  public int getId() {
    return id;
  }

  public String getBusiness() {
    return business;
  }

  public List<String> getDimension() {
    return dimensions;
  }

  public List<String> getMetrics() {
    return metrics;
  }

}
