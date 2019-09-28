package com.weibo.dip.warehouse;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang.StringUtils;

/** @author yurun */
public class DiffMain {
  public static Set<String> getDiff1() throws Exception {
    Set<String> lines = new HashSet<>();

    BufferedReader reader = null;

    try {
      reader =
          new BufferedReader(
              new InputStreamReader(
                  AliyunLogRegexMain.class.getClassLoader().getResourceAsStream("diff1")));

      String line;

      while ((line = reader.readLine()) != null) {
        if (StringUtils.isNotEmpty(line)) {
          lines.add(line.trim());
        }
      }
    } finally {
      if (Objects.nonNull(reader)) {
        reader.close();
      }
    }

    return lines;
  }

  public static Set<String> getDiff2() throws Exception {
    Set<String> lines = new HashSet<>();

    BufferedReader reader = null;

    try {
      reader =
          new BufferedReader(
              new InputStreamReader(
                  AliyunLogRegexMain.class.getClassLoader().getResourceAsStream("diff2")));

      String line;

      while ((line = reader.readLine()) != null) {
        if (StringUtils.isNotEmpty(line)) {
          lines.add(line.trim());
        }
      }
    } finally {
      if (Objects.nonNull(reader)) {
        reader.close();
      }
    }

    return lines;
  }

  public static void main(String[] args) throws Exception {
    Set<String> diff1 = getDiff1();
    Set<String> diff2 = getDiff2();

    for (String line : diff1) {
      if (!diff2.contains(line)) {
        System.out.println(line);
      }
    }
  }
}
