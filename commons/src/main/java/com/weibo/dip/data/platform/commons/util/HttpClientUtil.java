package com.weibo.dip.data.platform.commons.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.lang.CharEncoding;
import org.apache.http.client.params.CookiePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClientUtil {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(HttpClientUtil.class);

  private static final int CONNECT_TIMEOUT = 3000;// MS 3s

  private static final int SO_TIMEOUT = 10 * 60 * 1000;// MS 10min

  private static String read(InputStream in) throws IOException {
    if (in == null) {
      return null;
    }

    List<String> lines = new ArrayList<>();

    BufferedReader reader = new BufferedReader(new InputStreamReader(in, CharEncoding.UTF_8));

    String line;

    while ((line = reader.readLine()) != null) {
      lines.add(line);
    }

    StringBuilder sb = new StringBuilder();

    Iterator<String> iterator = lines.iterator();

    while (iterator.hasNext()) {
      sb.append(iterator.next());

      if (iterator.hasNext()) {
        sb.append("\n");
      }
    }

    return sb.toString();
  }

  public static String doGet(String url) throws IOException {
    HttpClient client = new HttpClient();

    client.getHttpConnectionManager().getParams()
        .setConnectionTimeout(CONNECT_TIMEOUT);
    client.getHttpConnectionManager().getParams().setSoTimeout(SO_TIMEOUT);

    GetMethod method = new GetMethod(url);

    String response;

    try {
      client.executeMethod(method);

      response = read(method.getResponseBodyAsStream());
    } finally {
      method.releaseConnection();
    }

    return response;
  }

  public static String doGet(String url, Map<String, String> params)
      throws IOException {
    StringBuilder buffer = new StringBuilder(url);

    if (MapUtils.isNotEmpty(params)) {
      int count = 0;

      for (Map.Entry<String, String> entry : params.entrySet()) {
        if (count++ == 0) {
          buffer.append("?").append(URLEncoder.encode(entry.getKey(), CharEncoding.UTF_8)).append("=").append(URLEncoder.encode(entry.getValue(), CharEncoding.UTF_8));
        } else {
          buffer.append("&").append(URLEncoder.encode(entry.getKey(), CharEncoding.UTF_8)).append("=").append(URLEncoder.encode(entry.getValue(), CharEncoding.UTF_8));
        }
      }
    }

    return doGet(buffer.toString());
  }

  public static String doNativeGet(String url, Map<String, String> params)
      throws IOException {
    StringBuilder buffer = new StringBuilder(url);

    if (MapUtils.isNotEmpty(params)) {
      int count = 0;

      for (Map.Entry<String, String> entry : params.entrySet()) {
        if (count++ == 0) {
          buffer.append("?").append(entry.getKey()).append("=").append(entry.getValue());
        } else {
          buffer.append("&").append(entry.getKey()).append("=").append(entry.getValue());
        }
      }
    }

    return doGet(buffer.toString());
  }

  public static String doPost(String url, Map<String, String> params)
      throws IOException {
    HttpClient client = new HttpClient();

    client.getHttpConnectionManager().getParams()
        .setConnectionTimeout(CONNECT_TIMEOUT);
    client.getHttpConnectionManager().getParams().setSoTimeout(SO_TIMEOUT);
    client.getParams().setParameter(HttpMethodParams.HTTP_CONTENT_CHARSET,
        CharEncoding.UTF_8);
    client.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
        new DefaultHttpMethodRetryHandler(0, false));
    client.getParams().setParameter("http.protocol.cookie-policy",
        CookiePolicy.BROWSER_COMPATIBILITY);

    PostMethod method = new PostMethod(url);

    String response;

    if (MapUtils.isNotEmpty(params)) {
      List<NameValuePair> pairs = new ArrayList<>();

      for (Map.Entry<String, String> entry : params.entrySet()) {
        pairs.add(new NameValuePair(entry.getKey(), entry.getValue()));
      }

      method.setRequestBody(pairs.toArray(new NameValuePair[pairs.size()]));
    }

    try {
      client.executeMethod(method);

      response = read(method.getResponseBodyAsStream());
    } finally {
      method.releaseConnection();
    }

    return response;
  }

  public static String doPost(String url, String json)
      throws IOException {
    HttpClient client = new HttpClient();

    client.getHttpConnectionManager().getParams()
        .setConnectionTimeout(CONNECT_TIMEOUT);
    client.getHttpConnectionManager().getParams().setSoTimeout(SO_TIMEOUT);
    client.getParams().setParameter(HttpMethodParams.HTTP_CONTENT_CHARSET,
        CharEncoding.UTF_8);
    client.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
        new DefaultHttpMethodRetryHandler(0, false));
    client.getParams().setParameter("http.protocol.cookie-policy",
        CookiePolicy.BROWSER_COMPATIBILITY);

    StringRequestEntity requestEntity = new StringRequestEntity(json, "application/json", CharEncoding.UTF_8);

    PostMethod method = new PostMethod(url);

    method.setRequestEntity(requestEntity);

    String response;

    try {
      client.executeMethod(method);

      response = read(method.getResponseBodyAsStream());
    } finally {
      method.releaseConnection();
    }

    return response;
  }

  @Deprecated
  public static String doGet(String url, String responseEncode) throws IOException {
    String response = doGet(url);
    if (Objects.isNull(response)) {
      return null;
    }

    return new String(response.getBytes(), responseEncode);
  }

}
