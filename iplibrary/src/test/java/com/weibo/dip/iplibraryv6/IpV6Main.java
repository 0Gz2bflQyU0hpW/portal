package com.weibo.dip.iplibraryv6;

import java.net.InetAddress;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.validator.routines.InetAddressValidator;
import sun.net.util.IPAddressUtil;

/** @author yurun */
public class IpV6Main {
  public static void main(String[] args) throws Exception {
    System.out.println(InetAddress.getByName("::").getHostAddress());

    InetAddressValidator validator = InetAddressValidator.getInstance();

    System.out.println(validator.isValid("0:0:0:0:0:0:0:1"));
    System.out.println(validator.isValidInet4Address("0:0:0:0:0:0:0:1"));
    System.out.println(validator.isValidInet6Address("0:0:0:0:0:0:0:1"));

    System.out.println(IPAddressUtil.textToNumericFormatV6("2001:209::").length);

    System.out.println(
        DatatypeConverter.printHexBinary(IPAddressUtil.textToNumericFormatV6("2001:209::")));
  }
}
