package com.weibo.dip.web.model;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * Created by haisen on 2018/4/16.
 */
public class Searching {

  private String condition;
  private String starttime;
  private String endtime;
  private String keyword;

  /**
   * 缺省构造函数.
   */
  public Searching() {
    condition = "";
    starttime = "";
    endtime = "";
    keyword = "";
  }

  /**
   * 构造函数.
   *
   * @param condition 查询条件
   * @param starttime 开始时间
   * @param endtime 结束时间
   * @param keyword 关键字
   */
  public Searching(String condition, String starttime, String endtime, String keyword) {
    this.condition = condition;
    this.starttime = starttime;
    this.endtime = endtime;
    this.keyword = keyword;
  }

  public String getCondition() {
    return condition;
  }

  public String getKeyword() {
    return keyword;
  }

  /**
   * 返回开始时间.
   *
   * @return 返回处理之后的开始时间
   */
  public String getStarttime() {
    if (starttime.equals("")) {
      return getstarttimeString();
    }
    return starttime;
  }

  /**
   * 返回结束时间.
   *
   * @return 返回处理之后的结束时间
   */
  public String getEndtime() {
    if (endtime.equals("")) {
      return getendtimeString();
    }
    return endtime;
  }

  public void setCondition(String condition) {
    this.condition = condition;
  }

  public void setStarttime(String starttime) {
    this.starttime = starttime;
  }

  public void setEndtime(String endtime) {
    this.endtime = endtime;
  }

  public void setKeyword(String keyword) {
    this.keyword = keyword;
  }

  private String timeToString(SimpleDateFormat simpleDateFormat, Date date) {
    return simpleDateFormat.format(date);
  }

  private String getstarttimeString() {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-dd HH:MM:ss", Locale.FRANCE);
    return this.getstarttimeString(simpleDateFormat);
  }

  private String getstarttimeString(SimpleDateFormat simpleDateFormat) {
    Timestamp date = new Timestamp(0);
    return timeToString(simpleDateFormat, date);
  }

  private String getendtimeString() {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-dd HH:MM:ss", Locale.FRANCE);
    return this.getendtimeString(simpleDateFormat);
  }

  private String getendtimeString(SimpleDateFormat simpleDateFormat) {

    //得到一天时间的最晚时间，因为得到当前时间存在差值，没办法-_-,有待修改
    Calendar today = Calendar.getInstance();
    today.set(Calendar.HOUR_OF_DAY, 23);
    today.set(Calendar.MINUTE, 59);
    today.set(Calendar.MILLISECOND, 999);
    today.set(Calendar.SECOND, 59);
    Timestamp date;
    date = new Timestamp(today.getTime().getTime());
    return timeToString(simpleDateFormat, date);
  }

  /**
   * 根据condition判断是否存在.
   *
   * @return 是否存在
   */
  public boolean isExit() {
    if (condition != null && condition.length() > 0) {
      return true;
    }
    return false;
  }

}
