package com.weibo.dip.web.model.intelligentwarning;

/**
 * Created by shixi_ruibo on 2018/3/13.
 */
public class Blacklist {

  //id，主键，自动递增
  private int id;

  //黑名单名称
  private String blackName;

  //业务名称
  private String business;

  //维度条件
  private String dimensions;

  //异常数据开始时间
  private String beginTime;

  //异常数据结束时间
  private String finishTime;

  //黑名单更新日期，最近创建或修改的时间戳
  private long updateTime;

  public Blacklist() {
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
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

  public String getBlackName() {
    return blackName;
  }

  public void setBlackName(String blackName) {
    this.blackName = blackName;
  }

  public String getBeginTime() {
    return beginTime;
  }

  public void setBeginTime(String beginTime) {
    this.beginTime = beginTime;
  }

  public String getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(String finishTime) {
    this.finishTime = finishTime;
  }

  public long getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(long updateTime) {
    this.updateTime = updateTime;
  }


}
