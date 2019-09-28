package com.weibo.dip.warehouse;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author yurun */
public class WWWLogRegexMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(WWWLogRegexMain.class);

  private static String getLine() throws Exception {
    BufferedReader reader = null;

    try {
      reader =
          new BufferedReader(
              new InputStreamReader(
                  WWWLogRegexMain.class.getClassLoader().getResourceAsStream("line")));

      return reader.readLine();
    } finally {
      if (Objects.nonNull(reader)) {
        reader.close();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    String regex =
        "^_accesskey=([^=]*)&_ip=([^=]*)&_port=([^=]*)&_an=([^=]*)&_data=([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) \\[([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) (.*)$";

    Pattern pattern = Pattern.compile(regex);

    Matcher matcher = pattern.matcher(getLine());

    LOGGER.info("match: {}", matcher.matches());
  }
}
