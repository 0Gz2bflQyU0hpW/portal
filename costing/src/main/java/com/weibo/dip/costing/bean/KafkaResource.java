package com.weibo.dip.costing.bean;

import java.util.Date;

/**
 * Created by yurun on 18/4/25.
 */
public class KafkaResource {

  private int id;
  private String topicName;
  private String productUuid;
  private double peakBytesout;
  private String comment;
  private Date collectTime;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public String getProductUuid() {
    return productUuid;
  }

  public void setProductUuid(String productUuid) {
    this.productUuid = productUuid;
  }

  public double getPeakBytesout() {
    return peakBytesout;
  }

  public void setPeakBytesout(double peakBytesout) {
    this.peakBytesout = peakBytesout;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public Date getCollectTime() {
    return collectTime;
  }

  public void setCollectTime(Date collectTime) {
    this.collectTime = collectTime;
  }

}
