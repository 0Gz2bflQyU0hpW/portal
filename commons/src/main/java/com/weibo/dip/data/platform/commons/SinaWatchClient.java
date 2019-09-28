package com.weibo.dip.data.platform.commons;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sina watch client.
 *
 * @author yurun
 */
public class SinaWatchClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(SinaWatchClient.class);

  private static final String HOST = "iconnect.monitor.sina.com.cn";
  private static final int PORT = 80;

  private static final int TIMEOUT = 10;

  private static final String KID = "2012090416";
  private static final String PASSWD = "VrcUe70eTvnGIm3wd4g6cLtMlvg7s7";

  private static final String SV = "DIP";

  private static final String SEND = "/v1/alert/send";

  private static final boolean AUTH = true;

  private static final String ALERT = "alert";
  private static final String REPORT = "report";

  public SinaWatchClient() {}

  private String md5(String body) throws Exception {
    char[] hexDigits = {
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    byte[] btInput = body.getBytes();

    MessageDigest mdInst = MessageDigest.getInstance("MD5");
    mdInst.update(btInput);

    byte[] md = mdInst.digest();

    char[] str = new char[md.length * 2];

    int index = 0;

    for (byte byte0 : md) {
      str[index++] = hexDigits[byte0 >>> 4 & 0xf];
      str[index++] = hexDigits[byte0 & 0xf];
    }

    return new String(str).toLowerCase();
  }

  private void hash(HttpRequestBase request) throws Exception {
    // content-type
    String contentType = "application/x-www-form-urlencoded";

    Header[] headers = request.getHeaders("Content-Type");
    if (headers.length > 0) {
      contentType = headers[0].getValue();
    } else {
      request.addHeader("Content-Type", contentType);
    }

    // md5
    String contentMd5 = "";
    if (request instanceof HttpPost) {
      HttpEntity entity = ((HttpPost) request).getEntity();
      String body = EntityUtils.toString(entity);

      contentMd5 = md5(body);
      request.addHeader("Content-MD5", contentMd5);
    }

    // expire
    long expireTime = System.currentTimeMillis() / 60 + 60 * 10;
    request.addHeader("Expires", String.valueOf(expireTime));

    // hash
    String method = request.getRequestLine().getMethod().toUpperCase();

    // hard coded, make my life easier...
    String canonicalizedamzheaders = "";
    String canonicalizedresource = request.getURI().getPath();
    if (request.getURI().getQuery() != null && request.getURI().getQuery().length() > 0) {
      canonicalizedresource += "?" + request.getURI().getQuery();
    }

    String stringToSign =
        method
            + "\n"
            + contentMd5
            + "\n"
            + contentType
            + "\n"
            + expireTime
            + "\n"
            + canonicalizedamzheaders
            + canonicalizedresource;

    request.addHeader("Authorization", "sinawatch " + KID + ":" + sign(stringToSign, PASSWD));
  }

  private String sign(String stringToSign, String passwd) {
    String algorithm = "HmacSHA1";
    try {
      Mac mac = Mac.getInstance(algorithm);
      mac.init(new SecretKeySpec(passwd.getBytes("UTF-8"), algorithm));
      byte[] signature = mac.doFinal(stringToSign.getBytes("UTF-8"));
      signature = Base64.encodeBase64(signature);

      return new String(signature).substring(5, 15);
    } catch (Exception e) {
      e.printStackTrace();
      // no way to raise exception here.
      return "";
    }
  }

  private String post(String url, List<NameValuePair> parameters) throws Exception {
    String uri = "http://" + HOST + ":" + PORT + url;

    HttpPost request = new HttpPost(uri);

    HttpEntity entity = new UrlEncodedFormEntity(parameters, "UTF-8");
    request.setEntity(entity);

    return fetch(request);
  }

  private String fetch(HttpRequestBase request) throws Exception {
    HttpParams httpParams = new BasicHttpParams();
    HttpConnectionParams.setConnectionTimeout(httpParams, TIMEOUT * 1000);

    HttpClient client = new DefaultHttpClient(httpParams);

    try {
      if (AUTH) {
        hash(request);
      }

      // Create a response handler
      ResponseHandler<String> responseHandler =
          response -> {
            int status = response.getStatusLine().getStatusCode();

            if (status == 200) {
              HttpEntity entity = response.getEntity();

              return entity != null ? EntityUtils.toString(entity) : null;
            } else {
              throw new ClientProtocolException("Unexpected response status: " + status);
            }
          };

      return client.execute(request, responseHandler);
    } finally {
      client.getConnectionManager().shutdown();
    }
  }

  private void send(
      String type, String service, String object, String subject, String content, String... tos) {
    LOGGER.info(
        "type: {}, service: {}, object: {}, subject: {}, content: {}, tos: {}",
        type,
        service,
        object,
        subject,
        content,
        tos);

    if (ArrayUtils.isEmpty(tos)) {
      return;
    }

    List<NameValuePair> parameters = new ArrayList<>();

    parameters.add(new BasicNameValuePair("sv", SV)); // 产品线名称

    parameters.add(new BasicNameValuePair("service", service)); // 产品线下的服务
    parameters.add(new BasicNameValuePair("object", object)); // 服务下的某个部分

    parameters.add(new BasicNameValuePair("subject", subject)); // 报警的标题，短信的内容
    parameters.add(new BasicNameValuePair("content", content)); // 邮件报警的邮件内容

    String sendTo = StringUtils.join(tos, Symbols.COMMA);

    parameters.add(new BasicNameValuePair("mailto", sendTo)); // 邮件接收人
    parameters.add(new BasicNameValuePair("gmailto", sendTo)); // 邮件接收人组

    if (type.equals(ALERT)) {
      parameters.add(new BasicNameValuePair("msgto", sendTo)); // 短信接收人
      parameters.add(new BasicNameValuePair("gmsgto", sendTo)); // 短信接收人组

      parameters.add(new BasicNameValuePair("wechatto", sendTo)); // 微信接收人
      parameters.add(new BasicNameValuePair("gwechatto", sendTo)); // 微信接收人组
    }

    parameters.add(new BasicNameValuePair("auto_merge", "0")); // 开启/关闭自动合并，默认为开启

    try {
      post(SEND, parameters);
    } catch (Throwable e) {
      LOGGER.error("sina watch client post(alert) error: {}", ExceptionUtils.getFullStackTrace(e));
    }
  }

  /**
   * Alert to users or groups, can be used in conjunction.
   *
   * @param service service name
   * @param object sub service name
   * @param subject subject
   * @param content content
   * @param tos users or groups
   */
  public void alert(String service, String object, String subject, String content, String... tos) {
    send(ALERT, service, object, subject, content, tos);
  }

  /**
   * Report to users or groups, can be used in conjunction.
   *
   * @param service service name
   * @param object sub service name
   * @param subject subject
   * @param content content
   * @param tos users or groups
   */
  public void report(String service, String object, String subject, String content, String... tos) {
    send(REPORT, service, object, subject, content, tos);
  }
}
