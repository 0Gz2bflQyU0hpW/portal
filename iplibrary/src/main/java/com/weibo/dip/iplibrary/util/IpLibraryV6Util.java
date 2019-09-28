package com.weibo.dip.iplibrary.util;

import com.weibo.dip.iplibrary.IpLibraryV6;
import com.weibo.dip.iplibrary.Location;
import java.util.Objects;

/**
 * Ip library util.
 *
 * @author yurun
 */
public class IpLibraryV6Util {
  /**
   * Main.
   *
   * @param args args[0] ip address
   * @throws Exception if ip library get error
   */
  public static void main(String[] args) throws Exception {
    IpLibraryV6 ipLibrary = new IpLibraryV6();

    Location location = ipLibrary.getLocation(args[0]);

    System.out.println(Objects.nonNull(location) ? location.toString() : "null");

    ipLibrary.close();
  }
}
