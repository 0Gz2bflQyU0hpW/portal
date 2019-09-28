package com.weibo.dip.warehouse;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author yurun */
public class AliyunLogRegexMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(AliyunLogRegexMain.class);

  private static String getLine() throws Exception {
    BufferedReader reader = null;

    try {
      reader =
          new BufferedReader(
              new InputStreamReader(
                  AliyunLogRegexMain.class.getClassLoader().getResourceAsStream("aliyun")));

      return reader.readLine();
    } finally {
      if (Objects.nonNull(reader)) {
        reader.close();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    String regex =
        "^([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) \\[([^\\s]*) ([^\\s]*)] \"([^\\s]*) ([^\\s]*) ([^\\s]*)\" ([^\\s]*) ([^\\s]*) \"([^\\s]*)\" \"([^\\s]*)\" \"([^\\s]*)\" \"(.*)\"$";

    System.out.println(regex);

    Pattern pattern = Pattern.compile(regex);

    Matcher matcher = pattern.matcher(getLine());

    if (matcher.matches()) {
      for (int index = 1; index <= matcher.groupCount(); index++) {
        System.out.println(matcher.group(index));
      }

    } else {
      System.out.println("match false");
    }
  }
}
