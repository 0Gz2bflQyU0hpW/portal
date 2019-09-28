package com.weibo.dip.web.model.datamart;

import java.util.Date;

/**
 * Created by shixi_dongxue3 on 2018/1/31.
 */
public class KafkaConsumer {

  //id，主键，自动递增
  private int id;

  //产品线
  private String consumerProduct;

  //topic名称，和消费者组的组合唯一
  private String topicName;

  //消费者组
  private String consumerGroup;

  //创建时间，创建时自动生成
  private Date createTime;

  //最近一次更新的时间，更新时自动生成
  private Date updateTime;

  //联系人(邮箱前缀，多人用逗号隔开)
  private String contactPerson;

  //备注在哪个集群创建的
  private String comment;

  public KafkaConsumer() {
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getConsumerProduct() {
    return consumerProduct;
  }

  public void setConsumerProduct(String consumerProduct) {
    this.consumerProduct = consumerProduct;
  }

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public String getConsumerGroup() {
    return consumerGroup;
  }

  public void setConsumerGroup(String consumerGroup) {
    this.consumerGroup = consumerGroup;
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
}
