package com.weibo.dip.baishanyundownload.log;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Baishanyun domain log detail.
 *
 * @author yurun
 */
public class DomainLogDetail {
  private static final Pattern URL_PATTERN =
      Pattern.compile("http://([^?]*)/([^/]*)/([^?]*)\\?(.*)");

  private String domain;
  private String date;
  private String type;
  private String url;
  private String md5;
  private long size;

  public DomainLogDetail() {}

  /**
   * Construct a instance.
   *
   * @param domain domain
   * @param date date
   * @param type type
   * @param url url
   * @param md5 md5
   * @param size size
   */
  public DomainLogDetail(
      String domain, String date, String type, String url, String md5, long size) {
    this.domain = domain;
    this.date = date;
    this.type = type;
    this.url = url;
    this.md5 = md5;
    this.size = size;
  }

  public String getDomain() {
    return domain;
  }

  public void setDomain(String domain) {
    this.domain = domain;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getMd5() {
    return md5;
  }

  public void setMd5(String md5) {
    this.md5 = md5;
  }

  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  /**
   * Get log name.
   *
   * @return log name
   */
  public String getLogName() {
    Matcher matcher = URL_PATTERN.matcher(url);

    matcher.matches();

    return matcher.group(2) + "-" + matcher.group(3);
  }

  @Override
  public String toString() {
    return "DomainLogDetail{"
        + "domain='"
        + domain
        + '\''
        + ", date='"
        + date
        + '\''
        + ", type='"
        + type
        + '\''
        + ", url='"
        + url
        + '\''
        + ", md5='"
        + md5
        + '\''
        + ", size="
        + size
        + '}';
  }
}
