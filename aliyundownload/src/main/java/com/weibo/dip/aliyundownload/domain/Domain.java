package com.weibo.dip.aliyundownload.domain;

/**
 * Ali domain.
 *
 * @author yurun
 */
public class Domain {
  private String cname;
  private String description;
  private String cdnType;
  private String sourceType;
  private String domainStatus;
  private String sslProtocol;
  private String domainName;
  private String[] sources;
  private String gmtModified;
  private String sandbox;
  private String gmtCreated;

  /**
   * Construct a Domain instance.
   *
   * @param cname cname
   * @param description description
   * @param cdnType cdn type
   * @param sourceType source type
   * @param domainStatus domain status
   * @param sslProtocol ssl protocol
   * @param domainName domain name
   * @param sources sources
   * @param gmtModified gmt modified
   * @param sandbox sandbox
   * @param gmtCreated gmt created
   */
  public Domain(
      String cname,
      String description,
      String cdnType,
      String sourceType,
      String domainStatus,
      String sslProtocol,
      String domainName,
      String[] sources,
      String gmtModified,
      String sandbox,
      String gmtCreated) {
    this.cname = cname;
    this.description = description;
    this.cdnType = cdnType;
    this.sourceType = sourceType;
    this.domainStatus = domainStatus;
    this.sslProtocol = sslProtocol;
    this.domainName = domainName;
    this.sources = sources;
    this.gmtModified = gmtModified;
    this.sandbox = sandbox;
    this.gmtCreated = gmtCreated;
  }

  public String getCname() {
    return cname;
  }

  public void setCname(String cname) {
    this.cname = cname;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getCdnType() {
    return cdnType;
  }

  public void setCdnType(String cdnType) {
    this.cdnType = cdnType;
  }

  public String getSourceType() {
    return sourceType;
  }

  public void setSourceType(String sourceType) {
    this.sourceType = sourceType;
  }

  public String getDomainStatus() {
    return domainStatus;
  }

  public void setDomainStatus(String domainStatus) {
    this.domainStatus = domainStatus;
  }

  public String getSslProtocol() {
    return sslProtocol;
  }

  public void setSslProtocol(String sslProtocol) {
    this.sslProtocol = sslProtocol;
  }

  public String getDomainName() {
    return domainName;
  }

  public void setDomainName(String domainName) {
    this.domainName = domainName;
  }

  public String[] getSources() {
    return sources;
  }

  public void setSources(String[] sources) {
    this.sources = sources;
  }

  public String getGmtModified() {
    return gmtModified;
  }

  public void setGmtModified(String gmtModified) {
    this.gmtModified = gmtModified;
  }

  public String getSandbox() {
    return sandbox;
  }

  public void setSandbox(String sandbox) {
    this.sandbox = sandbox;
  }

  public String getGmtCreated() {
    return gmtCreated;
  }

  public void setGmtCreated(String gmtCreated) {
    this.gmtCreated = gmtCreated;
  }
}
