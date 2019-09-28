package com.weibo.dip.aliyundownload.log;

/**
 * Aliyun domain log detail.
 *
 * @author yurun
 */
public class DomainLogDetail {

  private String domain;
  private String logName;
  private String logPath;
  private String startTime;
  private String endTime;
  private long logSize;

  /**
   * Create DomainLogDetail.
   *
   * @param domain domain name
   * @param logName log name
   * @param logPath log path
   * @param startTime log start time(include)
   * @param endTime log end time(exclude)
   * @param logSize log size
   */
  public DomainLogDetail(
      String domain,
      String logName,
      String logPath,
      String startTime,
      String endTime,
      long logSize) {
    this.domain = domain;
    this.logName = logName;
    this.logPath = logPath;
    this.startTime = startTime;
    this.endTime = endTime;
    this.logSize = logSize;
  }

  public String getDomain() {
    return domain;
  }

  public String getLogName() {
    return logName;
  }

  public String getLogPath() {
    return logPath;
  }

  public String getStartTime() {
    return startTime;
  }

  public long getLogSize() {
    return logSize;
  }

  @Override
  public String toString() {
    return "DomainLogDetail{"
        + "domain='"
        + domain
        + '\''
        + ", logName='"
        + logName
        + '\''
        + ", logPath='"
        + logPath
        + '\''
        + ", startTime='"
        + startTime
        + '\''
        + ", endTime='"
        + endTime
        + '\''
        + ", logSize="
        + logSize
        + '}';
  }
}
