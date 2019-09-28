package com.weibo.dip.web.model.datamart;

import java.util.Date;

/**
 * Created by shixi_dongxue3 on 2018/1/31.
 */
public class KafkaTopic {

  //id，主键，自动递增
  private int id;

  //topic名称，唯一
  private String topicName;

  //产品线
  private String product;

  //峰值流量大小(byte/s)
  private Integer peekTraffic;

  //峰值条数大小(条/s)
  private Integer peekQps;

  //topic数据量(条/day)
  private Integer datasizeNumber;

  //topic数据量(byte/day)
  private Integer datasizeSpace;

  //单条日志大小(byte)
  private Integer logsize;

  //创建时间，创建时自动生成
  private Date createTime;

  //最近一次更新topic的时间，更新时自动生成
  private Date updateTime;

  //联系人(邮箱前缀，多人用逗号隔开)
  private String contactPerson;

  //备注在哪个集群创建的
  private String comment;

  //集群名称
  private String clusterName;

  //日志描述
  private String logDescripton;

  public KafkaTopic() {
  }

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

  public String getProduct() {
    return product;
  }

  public void setProduct(String product) {
    this.product = product;
  }

  public Integer getPeekTraffic() {
    return peekTraffic;
  }

  public void setPeekTraffic(Integer peekTraffic) {
    this.peekTraffic = peekTraffic;
  }

  public Integer getPeekQps() {
    return peekQps;
  }

  public void setPeekQps(Integer peekQps) {
    this.peekQps = peekQps;
  }

  public Integer getDatasizeNumber() {
    return datasizeNumber;
  }

  public void setDatasizeNumber(Integer datasizeNumber) {
    this.datasizeNumber = datasizeNumber;
  }

  public Integer getDatasizeSpace() {
    return datasizeSpace;
  }

  public void setDatasizeSpace(Integer datasizeSpace) {
    this.datasizeSpace = datasizeSpace;
  }

  public Integer getLogsize() {
    return logsize;
  }

  public void setLogsize(Integer logsize) {
    this.logsize = logsize;
  }

  public Date getCreateTime() {
    return createTime;
  }

  public void setCreateTime(Date createTime) {
    this.createTime = createTime;
  }

  public Date getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(Date updateTime) {
    this.updateTime = updateTime;
  }

  public String getContactPerson() {
    return contactPerson;
  }

  public void setContactPerson(String contactPerson) {
    this.contactPerson = contactPerson;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public String getLogDescripton() {
    return logDescripton;
  }

  public void setLogDescripton(String logDescripton) {
    this.logDescripton = logDescripton;
  }
}
