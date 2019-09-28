package com.weibo.dip.iplibrary.util;

import com.weibo.dip.data.platform.commons.util.ShellUtil;
import org.apache.commons.lang.ArrayUtils;

/**
 * Ip lookup util.
 *
 * @author yurun
 */
public class IpLookupUtil {
  /**
   * 调用iplookup程序获取结果.
   *
   * <p>0：1表示正确获取结果，-1表示无结果
   *
   * <p>1：startip
   *
   * <p>2：endip
   *
   * <p>3：country
   *
   * <p>4：province
   *
   * <p>5: city
   *
   * <p>6:district
   *
   * <p>7:isp
   *
   * <p>8:type
   *
   * <p>9:desc
   *
   * @param ip address
   * @return iplookup result
   * @throws Exception if ip lookup error
   */
  public static String[] lookup(String ip) throws Exception {
    ShellUtil.Result record = ShellUtil.execute("/usr/local/bin/iplookup " + ip);

    if (record.isSuccess()) {
      String result = record.getOutput();

      String[] words = result.split("\t", -1);

      for (int index = 0; index < words.length; index++) {
        words[index] = words[index].trim();
      }

      return words;
    }

    return null;
  }

  public static void main(String[] args) throws Exception {
    System.out.println(ArrayUtils.toString(lookup(args[0])));
  }
}
