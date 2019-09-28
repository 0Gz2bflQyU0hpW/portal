package com.weibo.dip.iplibrary.util;

import com.weibo.dip.iplibrary.IpLibrary;
import com.weibo.dip.iplibrary.Location;
import java.util.Objects;

/**
 * Ip library util.
 *
 * @author yurun
 */
public class IpLibraryUtil {
  /**
   * Main.
   *
   * @param args args[0] ip address
   * @throws Exception if ip library get error
   */
  public static void main(String[] args) throws Exception {
    IpLibrary ipLibrary = new IpLibrary();

    Location location = ipLibrary.getLocation(args[0]);

    System.out.println(Objects.nonNull(location) ? location.toString() : "null");

    ipLibrary.close();
  }
}
