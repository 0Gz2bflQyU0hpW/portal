package com.weibo.dip.web.model.intelligentwarning;

/**
 * Created by shixi_dongxue3 on 2018/3/12.
 */
public class Strategy {

  //id，主键，自动递增
  private int id;

  //策略名称，唯一
  private String strategyName;

  //更新日期，最近创建或修改的时间戳
  private long updateTime;

  //业务名称
  private String business;

  //维度条件
  private String dimensions;

  //指标监控策略
  private String metrics;

  //报警人(邮箱前缀，多人用逗号隔开)
  private String contactPerson;

  //报警开关，有on/off两种状态
  private String status;

  public Strategy() {
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getStrategyName() {
    return strategyName;
  }

  public void setStrategyName(String strategyName) {
    this.strategyName = strategyName;
  }

  public long getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(long updateTime) {
    this.updateTime = updateTime;
  }

  public String getBusiness() {
    return business;
  }

  public void setBusiness(String business) {
    this.business = business;
  }

  public String getDimensions() {
    return dimensions;
  }

  public void setDimensions(String dimensions) {
    this.dimensions = dimensions;
  }

  public String getMetrics() {
    return metrics;
  }

  public void setMetrics(String metrics) {
    this.metrics = metrics;
  }

  public String getContactPerson() {
    return contactPerson;
  }

  public void setContactPerson(String contactPerson) {
    this.contactPerson = contactPerson;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }
}
