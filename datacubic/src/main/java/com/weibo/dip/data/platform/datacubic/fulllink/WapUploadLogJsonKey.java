package com.weibo.dip.data.platform.datacubic.fulllink;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance log json key.
 *
 * @author yurun
 */
@SuppressWarnings("Convert2Lambda")
public class WapUploadLogJsonKey {
  private static final Logger LOGGER = LoggerFactory.getLogger(WapUploadLogJsonKey.class);

  private static final Pattern PATTERN = Pattern.compile("^([^|]*)\\|(.*)$");

  /**
   * Main.
   *
   * @param args no params
   */
  public static void main(String[] args) {
    String inputPath = args[0];

    SparkConf conf = new SparkConf();

    JavaSparkContext context = new JavaSparkContext(conf);

    JavaRDD<String> sourceRdd = context.textFile(inputPath);

    List<String> keys =
        sourceRdd
            .flatMap(
                new FlatMapFunction<String, String>() {
                  @Override
                  public Iterator<String> call(String line) {
                    List<String> keys = new ArrayList<>();

                    Matcher matcher = PATTERN.matcher(line);
                    if (!matcher.matches()) {
                      return keys.iterator();
                    }

                    line = matcher.group(2);

                    JsonParser parser = new JsonParser();

                    try {
                      JsonElement element = parser.parse(line);

                      if (!element.isJsonObject()) {
                        return keys.iterator();
                      }

                      JsonObject jsonObject = element.getAsJsonObject();

                      Set<Map.Entry<String, JsonElement>> entries = jsonObject.entrySet();

                      for (Map.Entry<String, JsonElement> entry : entries) {
                        keys.add(entry.getKey());
                      }
                    } catch (JsonParseException e) {
                      LOGGER.error("json parse error: {}", ExceptionUtils.getFullStackTrace(e));
                    }

                    return keys.iterator();
                  }
                })
            .distinct()
            .collect();

    context.stop();

    for (String key : keys) {
      LOGGER.info(key);
    }
  }
}
