package com.weibo.dip.data.platform.commons.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS rawlog util.
 *
 * @author yurun
 */
public class RawlogUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(RawlogUtil.class);

  private static final String URL_FORMAT =
      "http://api.dip.sina.com.cn%s?category=%s&start=%s&end=%s&type=rawlog&accesskey=%s&timestamp=%s&ssig=%s";

  private static final JsonParser PARSER = new JsonParser();

  /**
   * Get range time files.
   *
   * @param category category
   * @param beginTime begin time(second, include)
   * @param endTime end time(second, exclude)
   * @return file paths
   * @throws Exception if error
   */
  public static List<String> getRangeTimeFiles(String category, Date beginTime, Date endTime)
      throws Exception {
    String rest = "/rest/v2/yunwei/getrangetimefiles";

    String start = String.valueOf(beginTime.getTime() / 1000 - 1);
    String end = String.valueOf(endTime.getTime() / 1000 - 1);

    Map<String, String> params = new HashMap<>();

    params.put("category", category);
    params.put("start", start);
    params.put("end", end);
    params.put("type", "rawlog");

    String accessKey = "qDkTu7X953A603C037C1";
    String timestamp = String.valueOf(System.currentTimeMillis());
    String secretkey = "4a61618a71f64cc2bb577bdbd6a7980ctFHNNSYf";

    String ssig = DipSsigUtil.getSsig(rest, params, accessKey, timestamp, secretkey);

    String response =
        HttpClientUtil.doGet(
            String.format(URL_FORMAT, rest, category, start, end, accessKey, timestamp, ssig));

    JsonObject object = PARSER.parse(response).getAsJsonObject();

    String code = object.getAsJsonPrimitive("code").getAsString();
    if (code.equals("200")) {
      JsonArray array = object.getAsJsonArray("data");

      List<String> files = new ArrayList<>(array.size());

      for (int index = 0; index < array.size(); index++) {
        files.add(array.get(index).getAsJsonPrimitive().getAsString());
      }

      return files;
    } else {
      LOGGER.warn(response);

      return null;
    }
  }
}
