package com.weibo.dip.aliyundownload;

import com.weibo.dip.aliyundownload.util.SignatureUtils;
import com.weibo.dip.data.platform.commons.util.HttpClientUtil;
import com.weibo.dip.data.platform.commons.util.UUIDUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.commons.lang3.time.FastTimeZone;

/** @author yurun */
public class GetDomainTester {
  private static final FastDateFormat ALIYUN_UTC_YYYY_MM_DD_T_HH_MM_SS_Z =
      FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ssZZ", FastTimeZone.getGmtTimeZone());

  private static final String ALIYUN_SERVICE = "https://cdn.aliyuncs.com/";

  public static void main(String[] args) throws Exception {
    String action = "DescribeUserDomains";
    String format = "JSON";
    String version = "2014-11-11";
    String signatureMethod = "HMAC-SHA1";
    String signatureVersion = "1.0";

    String accessKeyId = "LTAIxKzl1eARmD9r";
    String accessKeySecret = "tiPsFAfnqN8cBg2SLiCclQzGf5MpzC";

    Map<String, String> param = new HashMap<>();

    param.put("Action", action);
    param.put("PageNumber", "1");

    param.put("Format", format);
    param.put("Version", version);
    param.put("AccessKeyId", accessKeyId);
    param.put("SignatureMethod", signatureMethod);
    param.put("Timestamp", ALIYUN_UTC_YYYY_MM_DD_T_HH_MM_SS_Z.format(System.currentTimeMillis()));
    param.put("SignatureVersion", signatureVersion);
    param.put("SignatureNonce", UUIDUtil.getUUID());

    param.put("Signature", SignatureUtils.generate(param, accessKeySecret));

    String response = HttpClientUtil.doNativeGet(ALIYUN_SERVICE, param);

    System.out.println(response);
  }
}
