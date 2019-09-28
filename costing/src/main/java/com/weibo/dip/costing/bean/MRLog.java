package com.weibo.dip.costing.bean;

import java.util.Date;

/**
 * Created by yurun on 18/4/24.
 */
public class MRLog {

  private String jobid;
  private String jobname;
  private long hdfsBytesRead;
  private String[] categories;
  private Date createTime;

  public String getJobid() {
    return jobid;
  }

  public void setJobid(String jobid) {
    this.jobid = jobid;
  }

  public String getJobname() {
    return jobname;
  }

  public void setJobname(String jobname) {
    this.jobname = jobname;
  }

  public long getHdfsBytesRead() {
    return hdfsBytesRead;
  }

  public void setHdfsBytesRead(long hdfsBytesRead) {
    this.hdfsBytesRead = hdfsBytesRead;
  }

  public String[] getCategories() {
    return categories;
  }

  public void setCategories(String[] categories) {
    this.categories = categories;
  }

  public Date getCreateTime() {
    return createTime;
  }

  public void setCreateTime(Date createTime) {
    this.createTime = createTime;
  }

}
