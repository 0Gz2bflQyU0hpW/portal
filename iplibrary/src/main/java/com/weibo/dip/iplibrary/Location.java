package com.weibo.dip.iplibrary;

/**
 * IpLibrary location.
 *
 * @author yurun
 */
public class Location {
  private String country;
  private String province;
  private String city;
  private String district;
  private String isp;
  private String type;
  private String desc;

  public Location() {}

  /**
   * Construct a Location instance.
   *
   * @param country country name
   * @param province province name
   * @param city city name
   * @param district district name
   * @param isp isp name
   * @param type type name
   * @param desc desc info
   */
  public Location(
      String country,
      String province,
      String city,
      String district,
      String isp,
      String type,
      String desc) {
    this.country = country;

    this.province = province;

    this.city = city;

    this.district = district;

    this.isp = isp;

    this.type = type;

    this.desc = desc;
  }

  public String getCountry() {
    return country;
  }

  public void setCountry(String country) {
    this.country = country;
  }

  public String getProvince() {
    return province;
  }

  public void setProvince(String province) {
    this.province = province;
  }

  public String getCity() {
    return city;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public String getDistrict() {
    return district;
  }

  public void setDistrict(String district) {
    this.district = district;
  }

  public String getIsp() {
    return isp;
  }

  public void setIsp(String isp) {
    this.isp = isp;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getDesc() {
    return desc;
  }

  public void setDesc(String desc) {
    this.desc = desc;
  }

  @Override
  public String toString() {
    return String.format(
        "country: %s, province: %s, city: %s, district: %s, isp: %s, type: %s, desc: %s",
        country, province, city, district, isp, type, desc);
  }
}
