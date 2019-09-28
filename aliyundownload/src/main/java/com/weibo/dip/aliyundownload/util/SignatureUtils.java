package com.weibo.dip.aliyundownload.util;

import com.google.common.base.Preconditions;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aliyun signature utils.
 *
 * @author yurun
 */
public class SignatureUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SignatureUtils.class);

  private static final String CHARSET_UTF8 = "utf8";
  private static final String ALGORITHM = "UTF-8";
  private static final String SEPARATOR = "&";

  /**
   * Generate aliyun signature.
   *
   * @param method GET
   * @param parameter http get parameters
   * @param accessKeySecret Aliyun AccessKeySecret
   * @return signature
   * @throws Exception if generate failure
   */
  public static String generate(
      String method, Map<String, String> parameter, String accessKeySecret) throws Exception {
    String signString = generateSignString(method, parameter);

    byte[] signBytes = hmacSha1Signature(accessKeySecret + "&", signString);

    String signature = newStringByBase64(signBytes);

    Preconditions.checkState(StringUtils.isNotEmpty(signature), "signature can not be empty");

    if ("POST".equals(method)) {
      return signature;
    }

    return URLEncoder.encode(signature, "UTF-8");
  }

  public static String generate(Map<String, String> parameter, String accessKeySecret)
      throws Exception {
    return generate("GET", parameter, accessKeySecret);
  }

  /**
   * Generate sign string.
   *
   * @param httpMethod GET
   * @param parameter http get parameters
   * @return sign
   */
  public static String generateSignString(String httpMethod, Map<String, String> parameter) {
    Preconditions.checkState(StringUtils.isNotEmpty(httpMethod), "httpMethod can not be empty");

    TreeMap<String, String> sortParameter = new TreeMap<>(parameter);

    String canonicalQueryString = generateQueryString(sortParameter, true);

    return httpMethod
        + SEPARATOR
        + percentEncode("/")
        + SEPARATOR
        + percentEncode(canonicalQueryString);
  }

  /**
   * Generate query string.
   *
   * @param params http get parameters
   * @param isEncoded parameters is encoded?
   * @return query
   */
  public static String generateQueryString(Map<String, String> params, boolean isEncoded) {
    StringBuilder canonicalQueryString = new StringBuilder();

    for (Map.Entry<String, String> entry : params.entrySet()) {
      if (isEncoded) {
        canonicalQueryString
            .append(percentEncode(entry.getKey()))
            .append("=")
            .append(percentEncode(entry.getValue()))
            .append("&");
      } else {
        canonicalQueryString
            .append(entry.getKey())
            .append("=")
            .append(entry.getValue())
            .append("&");
      }
    }

    if (canonicalQueryString.length() > 1) {
      canonicalQueryString.setLength(canonicalQueryString.length() - 1);
    }

    return canonicalQueryString.toString();
  }

  /**
   * Percent encode.
   *
   * @param value value
   * @return encode value
   */
  public static String percentEncode(String value) {
    try {
      return value == null
          ? null
          : URLEncoder.encode(value, CHARSET_UTF8)
              .replace("+", "%20")
              .replace("*", "%2A")
              .replace("%7E", "~");
    } catch (Exception e) {
      LOGGER.error("percent encode error: {}", ExceptionUtils.getFullStackTrace(e));
    }

    return "";
  }

  /**
   * Generate HmacSHA1 signature.
   *
   * @param secret secret
   * @param baseString base string
   * @return signature
   * @throws Exception if failure
   */
  public static byte[] hmacSha1Signature(String secret, String baseString) throws Exception {
    Preconditions.checkState(StringUtils.isNotEmpty(secret), "secret can not be empty");
    Preconditions.checkState(StringUtils.isNotEmpty(baseString), "baseString can not be empty");

    Mac mac = Mac.getInstance("HmacSHA1");

    SecretKeySpec keySpec = new SecretKeySpec(secret.getBytes(CHARSET_UTF8), ALGORITHM);

    mac.init(keySpec);

    return mac.doFinal(baseString.getBytes(CHARSET_UTF8));
  }

  /**
   * Generate base64 string.
   *
   * @param bytes source byte array
   * @return base 64 string
   * @throws Exception if unsupported encode
   */
  public static String newStringByBase64(byte[] bytes) throws Exception {
    Preconditions.checkState(ArrayUtils.isNotEmpty(bytes), "bytes can not be empty");

    return new String(Base64.encodeBase64(bytes, false), CHARSET_UTF8);
  }

  /**
   * Example.
   *
   * @param args no args
   */
  public static void main(String[] args) {
    String action = "DescribeCdnDomainLogs";
    String domainName = "us.sinaimg.cn";
    // String logDay = "2018-05-14";

    String format = "JSON";
    String version = "2014-11-11";
    String accessKeyId = "LTAIxKzl1eARmD9r";
    String accessKeySecret = "tiPsFAfnqN8cBg2SLiCclQzGf5MpzC";
    String signatureMethod = "HMAC-SHA1";
    String signatureVersion = "1.0";

    Map<String, String> param = new HashMap<>();

    param.put("Action", action);
    param.put("DomainName", domainName);
    // param.put("LogDay", logDay);
    param.put("StartTime", "2018-04-26T15:00:00Z");
    param.put("EndTime", "2018-04-26T16:00:00Z");

    param.put("Format", format);
    param.put("Version", version);
    param.put("AccessKeyId", accessKeyId);
    param.put("SignatureMethod", signatureMethod);
    param.put("Timestamp", "2018-05-16T12:24:00Z");
    param.put("SignatureVersion", signatureVersion);
    param.put("SignatureNonce", "4444");

    try {
      String url = generate(param, accessKeySecret);
      System.out.println("url: " + url);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
