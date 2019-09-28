package com.weibo.dip.web.common;

/**
 * Created by yurun on 17/10/19.
 */
public class Constants {

  public static final String CAS_URL = "http://cas.erp.sina.com.cn/cas";
  public static final String CAS_LOGIN = CAS_URL + "/login?service=%s";
  public static final String CAS_LOGOUT = CAS_URL + "/logout?service=%s";
  public static final String CAS_VALIDATE = CAS_URL + "/validate?ticket=%s&service=%s";

  public static final String CAS_INFO = "info";
  public static final String CAS_USERNAME = "username";
  public static final String CAS_EMAIL = "email";

  public static final String THIS_URL = "http://offline.portal.dip.weibo.com:8080/cas/login";

  public static final String USER_NAME = "PORTAL_WEB_USER_NAME";

  public static final String USER_TICKET = "PORTAL_WEB_USER_TICKET";

  public static void main(String[] args) {
    System.out.println(String.format("%s%s", "a", "b"));
  }

}
