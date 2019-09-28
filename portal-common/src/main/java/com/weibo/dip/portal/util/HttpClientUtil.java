package com.weibo.dip.portal.util;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.*;
import java.util.Map.Entry;

/**
 * @author yurun
 * 
 * @datetime 2014-7-23 上午10:45:47
 */
public class HttpClientUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientUtil.class);

	private static final int CONNECT_TIMEOUT = 3000;// MS 3s

	private static final int SO_TIMEOUT = 10 * 60 * 1000;// MS 10min

	public static String doGet(String url) throws HttpException, IOException {
		LOGGER.info("do get url: " + url);

		if (StringUtils.isEmpty(url)) {
			LOGGER.error("get url is null or empty");

			return null;
		}

		HttpClient client = new HttpClient();
		
		client.getHttpConnectionManager().getParams().setConnectionTimeout(CONNECT_TIMEOUT);
		client.getHttpConnectionManager().getParams().setSoTimeout(SO_TIMEOUT);

		GetMethod method = new GetMethod(url);

		String response = null;

		try {
			client.executeMethod(method);

			response = method.getResponseBodyAsString();
		} finally {
			method.releaseConnection();
		}

		return response;
	}

	public static String doGet(String url, List<Pair<String, String>> params) throws HttpException, IOException {
		StringBuilder buffer = new StringBuilder(url);

		if (CollectionUtils.isNotEmpty(params)) {
			int count = 0;

			for (Pair<String, String> param : params) {
				if (count++ == 0) {
					buffer.append("?" + param.getFirst() + "=" + param.getSecond());
				} else {
					buffer.append("&" + param.getFirst() + "=" + param.getSecond());
				}
			}
		}

		return doGet(buffer.toString());
	}

	public static String doGet(String url, Map<String, String> params) throws HttpException, IOException {
		StringBuilder buffer = new StringBuilder(url);

		if (MapUtils.isNotEmpty(params)) {
			int count = 0;

			for (Entry<String, String> entry : params.entrySet()) {
				if (count++ == 0) {
					buffer.append("?" + entry.getKey() + "=" + entry.getValue());
				} else {
					buffer.append("&" + entry.getKey() + "=" + entry.getValue());
				}
			}
		}

		return doGet(buffer.toString());
	}

	public static String doEncodedGet(String url) throws HttpException, IOException {
		return doGet(URLEncoder.encode(url, CharEncoding.UTF_8));
	}

	public static String doEncodedGet(String url, List<Pair<String, String>> params) throws HttpException, IOException {
		StringBuilder buffer = new StringBuilder(url);

		if (CollectionUtils.isNotEmpty(params)) {
			int count = 0;

			for (Pair<String, String> param : params) {
				if (count++ == 0) {
					buffer.append(
							"?" + param.getFirst() + "=" + URLEncoder.encode(param.getSecond(), CharEncoding.UTF_8));
				} else {
					buffer.append(
							"&" + param.getFirst() + "=" + URLEncoder.encode(param.getSecond(), CharEncoding.UTF_8));
				}
			}
		}

		return doGet(buffer.toString());
	}

	public static String doEncodedGet(String url, Map<String, String> params) throws HttpException, IOException {
		StringBuilder buffer = new StringBuilder(url);

		if (MapUtils.isNotEmpty(params)) {
			int count = 0;

			for (Entry<String, String> entry : params.entrySet()) {
				if (count++ == 0) {
					buffer.append("?" + entry.getKey() + "=" + URLEncoder.encode(entry.getValue(), CharEncoding.UTF_8));
				} else {
					buffer.append("&" + entry.getKey() + "=" + URLEncoder.encode(entry.getValue(), CharEncoding.UTF_8));
				}
			}
		}

		return doGet(buffer.toString());
	}

	public static String doGet(String url, String responseEncode) throws HttpException, IOException {
		String response = doGet(url);

		return new String(response.getBytes(), responseEncode);
	}

	public static String doEncodedGet(String url, String responseEncode) throws HttpException, IOException {
		String response = doEncodedGet(url);

		return new String(response.getBytes(), responseEncode);
	}

	public static String doPost(String url, List<Pair<String, String>> params) throws HttpException, IOException {
		LOGGER.info("do post url: " + url + ", params: " + params);

		if (StringUtils.isEmpty(url)) {
			LOGGER.error("post url is null or empty");

			return null;
		}

		HttpClient client = new HttpClient();

		client.getHttpConnectionManager().getParams().setConnectionTimeout(CONNECT_TIMEOUT);
		client.getHttpConnectionManager().getParams().setSoTimeout(SO_TIMEOUT);
		client.getParams().setParameter(HttpMethodParams.HTTP_CONTENT_CHARSET, CharEncoding.UTF_8);
		client.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(0, false));
		client.getParams().setParameter("http.protocol.cookie-policy", CookiePolicy.BROWSER_COMPATIBILITY);

		PostMethod method = new PostMethod(url);

		String response = null;

		if (CollectionUtils.isNotEmpty(params)) {
			List<NameValuePair> pairs = new ArrayList<NameValuePair>();

			for (Pair<String, String> param : params) {
				if (param.getFirst() != null && !param.getFirst().isEmpty()) {
					NameValuePair pair = new NameValuePair(param.getFirst(), param.getSecond());

					pairs.add(pair);
				}
			}

			method.setRequestBody(pairs.toArray(new NameValuePair[0]));
		}

		try {
			client.executeMethod(method);

			response = read(method.getResponseBodyAsStream());
		} finally {
			method.releaseConnection();
		}

		return response;
	}

	public static String doEncodesPost(String url, List<Pair<String, String>> params)
			throws HttpException, IOException {
		List<Pair<String, String>> encodedParams = null;

		if (CollectionUtils.isNotEmpty(params)) {
			encodedParams = new ArrayList<Pair<String, String>>();

			for (Pair<String, String> param : params) {
				encodedParams.add(new Pair<String, String>(param.getFirst(),
						URLEncoder.encode(param.getSecond(), CharEncoding.UTF_8)));
			}
		}

		return doPost(url, encodedParams);
	}

	public static String doPost(String url, Map<String, String> params) throws HttpException, IOException {
		List<Pair<String, String>> tparams = null;

		if (MapUtils.isNotEmpty(params)) {
			tparams = new ArrayList<Pair<String, String>>();

			for (Entry<String, String> entry : params.entrySet()) {
				tparams.add(new Pair<String, String>(entry.getKey(), entry.getValue()));
			}
		}

		return doPost(url, tparams);
	}

	public static String doEncodesPost(String url, Map<String, String> params) throws HttpException, IOException {
		Map<String, String> encodedParams = null;

		if (MapUtils.isNotEmpty(params)) {
			encodedParams = new HashMap<String, String>();

			for (Entry<String, String> entry : params.entrySet()) {
				encodedParams.put(entry.getKey(), URLEncoder.encode(entry.getValue(), CharEncoding.UTF_8));
			}
		}

		return doPost(url, encodedParams);
	}

	public static String doPost(String url, Map<String, String> params, String responseEncode)
			throws HttpException, IOException {
		String response = doPost(url, params);

		return new String(response.getBytes(), responseEncode);
	}

	public static String doEncodedPost(String url, Map<String, String> params, String responseEncode)
			throws HttpException, IOException {
		String response = doEncodesPost(url, params);

		return new String(response.getBytes(), responseEncode);
	}

	private static String read(InputStream in) throws IOException {
		if (in == null) {
			return null;
		}

		List<String> lines = new ArrayList<String>();

		BufferedReader reader = new BufferedReader(new InputStreamReader(in));

		String line = null;

		while ((line = reader.readLine()) != null) {
			lines.add(line);
		}

		StringBuffer sb = new StringBuffer();

		Iterator<String> iterator = lines.iterator();

		while (iterator.hasNext()) {
			sb.append(iterator.next());

			if (iterator.hasNext()) {
				sb.append("\n");
			}
		}

		return sb.toString();
	}

}
