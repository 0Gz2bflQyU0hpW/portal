package com.weibo.dip.baishanyundownload.domain;

/**
 * Baishanyun domain.
 *
 * @author yurun
 */
public class Domain {
  private String id;
  private String domain;
  private String type;
  private String status;
  private String cname;
  private String icpStatus;
  private String icpNum;

  public Domain() {}

  /**
   * Construct a instance.
   *
   * @param id id
   * @param domain domain
   * @param type type
   * @param status status
   * @param cname cname
   * @param icpStatus icp status
   * @param icpNum icp num
   */
  public Domain(
      String id,
      String domain,
      String type,
      String status,
      String cname,
      String icpStatus,
      String icpNum) {
    this.id = id;
    this.domain = domain;
    this.type = type;
    this.status = status;
    this.cname = cname;
    this.icpStatus = icpStatus;
    this.icpNum = icpNum;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getDomain() {
    return domain;
  }

  public void setDomain(String domain) {
    this.domain = domain;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getCname() {
    return cname;
  }

  public void setCname(String cname) {
    this.cname = cname;
  }

  public String getIcpStatus() {
    return icpStatus;
  }

  public void setIcpStatus(String icpStatus) {
    this.icpStatus = icpStatus;
  }

  public String getIcpNum() {
    return icpNum;
  }

  public void setIcpNum(String icpNum) {
    this.icpNum = icpNum;
  }

  @Override
  public String toString() {
    return "Domain{"
        + "id='"
        + id
        + '\''
        + ", domain='"
        + domain
        + '\''
        + ", type='"
        + type
        + '\''
        + ", status='"
        + status
        + '\''
        + ", cname='"
        + cname
        + '\''
        + ", icpStatus='"
        + icpStatus
        + '\''
        + ", icpNum='"
        + icpNum
        + '\''
        + '}';
  }
}
