package com.weibo.dip.data.platform.datacubic.cdn.util;

import com.weibo.dip.iplibrary.IpLibrary;
import com.weibo.dip.iplibrary.Location;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.api.java.UDF1;

public class IpToLocation implements UDF1<String, Map<String, String>> {

  private static final String LIBRARY_DIR = "/data0/dipplat/software/systemfile/iplibrary";

  private static final Location OTHER = new Location("Other",
      "Other",
      "Other",
      "Other",
      "Other",
      "Other",
      "Other");

  private static final String COUNTRY = "country";

  private static final String PROVINCE = "province";

  private static final String CITY = "city";

  private static final String DISTRICT = "district";

  private static final String ISP = "isp";

  private static final String TYPE = "type";

  private static final String DESC = "desc";

  private static final IpLibrary ipLibrary;

  static {
    try {
      ipLibrary = new IpLibrary(LIBRARY_DIR);
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  @Override
  public Map<String, String> call(String ip) throws Exception {
    Location location = ipLibrary.getLocation(ip);

    if (location == null) {
      location = OTHER;
    }

    Map<String, String> result = new HashMap<>();

    result.put(COUNTRY, location.getCountry());
    result.put(PROVINCE, location.getProvince());
    result.put(CITY, location.getCity());
    result.put(DISTRICT, location.getDistrict());
    result.put(ISP, location.getIsp());
    result.put(TYPE, location.getType());
    result.put(DESC, location.getDesc());

    return result;
  }

}
