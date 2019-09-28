package com.weibo.dip.iplibrary.util;

import com.google.common.base.Preconditions;
import com.weibo.dip.iplibrary.IpLibrary;
import com.weibo.dip.iplibrary.Location;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.ArrayUtils;

/**
 * Check ip library.
 *
 * @author yurun
 */
public class CheckIpLibraryMain {
  private static void print(String ip, Location ipLocation, String[] ipLookupLocation) {
    System.out.println("ip: " + ip);
    System.out.println("iplibrary: " + ipLocation);
    System.out.println("iplookup: " + Arrays.toString(ipLookupLocation));
  }

  /**
   * Main.
   *
   * @param args no params
   * @throws Exception if check error
   */
  public static void main(String[] args) throws Exception {
    int number = Integer.valueOf(args[0]);

    List<String> ips = new ArrayList<>();

    for (int index = 0; index < number; index++) {
      StringBuilder ip = new StringBuilder();
      for (int i = 0; i < 4; i++) {
        int rand = (int) (Math.random() * 255);
        if (i != 3) {
          ip.append(rand).append(".");
        } else {
          ip.append(rand);
        }
      }

      ips.add(ip.toString());
    }

    IpLibrary iplibrary = new IpLibrary();

    int matchLine = 0;
    int exceptionCount = 0;

    for (String ip : ips) {
      Location ipLocation = iplibrary.getLocation(ip);
      String[] ipLookupLocation = IpLookupUtil.lookup(ip);

      Preconditions.checkState(
          ArrayUtils.isNotEmpty(ipLookupLocation), "iplookup %s, result is null", ip);

      if (ipLocation.getCountry().equals("内网")
          || (ipLocation.getCountry().equals(ipLookupLocation[3])
              && ipLocation.getProvince().equals(ipLookupLocation[4])
              && ipLocation.getCity().equals(ipLookupLocation[5])
              && ipLocation.getDistrict().equals(ipLookupLocation[6])
              && ipLocation.getIsp().equals(ipLookupLocation[7])
              && ipLocation.getType().equals(ipLookupLocation[8])
              && ipLocation.getDesc().equals(ipLookupLocation[9]))) {
        matchLine++;
      } else {
        exceptionCount++;

        print(ip, ipLocation, ipLookupLocation);
      }
    }

    iplibrary.close();

    System.out.println(
        "number: " + number + ", matchLine: " + matchLine + " exceptionCount: " + exceptionCount);
  }
}
