package com.weibo.dip.web.model.datamart;

import java.util.Date;

public class Dataset {

  //id，主键，自动递增
  private int id;

  //数据集名称，唯一
  private String datasetName;

  //产品线
  private String product;

  //创建日期，创建时自动生成
  private Date createTime;

  //更新日期，更新时自动生成
  private Date updateTime;

  //保存时间(天)
  private int storePeriod;

  //预估数据量大小(G/天)
  private float size;

  //联系人(邮箱前缀，多人用逗号隔开)
  private String contactPerson;

  //备注
  private String comment;

  public Dataset() {
  }

  /*getters and setters*/
  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getDatasetName() {
    return datasetName;
  }

  public void setDatasetName(String datasetName) {
    this.datasetName = datasetName;
  }

  public String getProduct() {
    return product;
  }

  public void setProduct(String product) {
    this.product = product;
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

  public int getStorePeriod() {
    return storePeriod;
  }

  public void setStorePeriod(int storePeriod) {
    this.storePeriod = storePeriod;
  }

  public float getSize() {
    return size;
  }

  public void setSize(float size) {
    this.size = size;
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
