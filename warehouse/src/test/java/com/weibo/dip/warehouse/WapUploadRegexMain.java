package com.weibo.dip.warehouse;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author yurun */
public class WapUploadRegexMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(WapUploadRegexMain.class);

  public static void main(String[] args) throws Exception {
    String line = "10.41.21.126|abc";
    String regex = "^([^|]*)\\|(.*)$";

    Pattern pattern = Pattern.compile(regex);

    Matcher matcher = pattern.matcher(line);

    LOGGER.info("match: {}", matcher.matches());
  }
}
