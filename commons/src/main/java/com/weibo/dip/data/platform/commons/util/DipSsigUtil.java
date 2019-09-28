package com.weibo.dip.data.platform.commons.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.CharEncoding;
import sun.misc.BASE64Encoder;

/**
 * 用于生成DIP平台所支持的SSIG生成算法.
 *
 * @author yurun
 */
public class DipSsigUtil {
  private DipSsigUtil() {}

  /**
   * Get ssig.
   *
   * @param url url
   * @param params get params
   * @param accessKey access key
   * @param timestamp timestamp
   * @param secretKey secret key
   * @return ssig
   * @throws NoSuchAlgorithmException if get HmacSHA1 error
   * @throws InvalidKeyException if SecretKeySpec init error
   * @throws UnsupportedEncodingException if encode error
   */
  public static String getSsig(
      String url, Map<String, String> params, String accessKey, String timestamp, String secretKey)
      throws NoSuchAlgorithmException, InvalidKeyException, UnsupportedEncodingException {
    String data;

    String prefix = "GET\n\n\n\n";

    if (MapUtils.isNotEmpty(params)) {
      StringBuilder paramStr = new StringBuilder();

      SortedMap<String, String> sortedParams = new TreeMap<>(params);

      for (Entry<String, String> param : sortedParams.entrySet()) {
        String name = param.getKey();
        String value = param.getValue();

        paramStr.append(name).append("=").append(value).append("&");
      }

      data =
          String.format(
              "%s%s?%saccesskey=%s&timestamp=%s",
              prefix, url, paramStr.toString(), accessKey, timestamp);
    } else {
      data = String.format("%s%s?accesskey=%s&timestamp=%s", prefix, url, accessKey, timestamp);
    }

    Mac mac = Mac.getInstance("HmacSHA1");

    SecretKeySpec spec = new SecretKeySpec(secretKey.getBytes(CharEncoding.UTF_8), "HmacSHA1");

    mac.init(spec);

    byte[] digest = mac.doFinal(data.getBytes(CharEncoding.UTF_8));

    BASE64Encoder encoder = new BASE64Encoder();

    data = encoder.encode(digest);
    if (data.length() > 15) {
      data = data.substring(5, 15);
    } else {
      data = data.substring(5);
    }

    return URLEncoder.encode(data, CharEncoding.UTF_8);
  }

  public static String getSsigPrefix(
      String httpVerb, String contentMd5, String contentType, String date) {

    return httpVerb + '\n' + contentMd5 + '\n' + contentType + '\n' + date + '\n';
  }
}
