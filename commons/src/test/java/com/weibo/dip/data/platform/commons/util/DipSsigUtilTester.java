package com.weibo.dip.data.platform.commons.util;

import java.util.HashMap;
import java.util.Map;

/** @author yurun */
public class DipSsigUtilTester {
  public static void main(String[] args) throws Exception {
    String url = "/rest/v2/yunwei/getrangetimefiles";

    Map<String, String> params = new HashMap<>();

    params.put("type", "rawlog");
    params.put("category", "app_picserversweibof6vwt_wapupload");
    params.put("start", "1537855200");
    params.put("end", "1537857260");

    String accessKey = "qDkTu7X953A603C037C1";
    String timestamp = "1409285460";
    String secretkey = "4a61618a71f64cc2bb577bdbd6a7980ctFHNNSYf";

    System.out.println(DipSsigUtil.getSsig(url, params, accessKey, timestamp, secretkey));
  }
}
