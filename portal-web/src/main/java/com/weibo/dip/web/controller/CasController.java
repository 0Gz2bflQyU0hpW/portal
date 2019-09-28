package com.weibo.dip.web.controller;

import com.weibo.dip.web.common.Constants;

import java.net.URL;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.jdom.Document;
import org.jdom.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * Created by yurun on 17/10/19.
 */
@Controller
@RequestMapping("/cas")
public class CasController {

  private static final Logger LOGGER = LoggerFactory.getLogger(CasController.class);

  private String getUsername(String ticket) {
    if (StringUtils.isEmpty(ticket)) {
      return null;
    }

    String email = null;

    try {
      URL url = new URL(String.format(Constants.CAS_VALIDATE, ticket, Constants.THIS_URL));

      SAXBuilder sax = new SAXBuilder();

      Document document = sax.build(url);

      String username = document.getRootElement().getChild(Constants.CAS_INFO)
          .getChild(Constants.CAS_USERNAME).getText();

      if (StringUtils.isNotEmpty(username)) {
        email = document.getRootElement().getChild(Constants.CAS_INFO)
            .getChild(Constants.CAS_EMAIL).getText();
      }
    } catch (Exception e) {
      email = null;

      LOGGER.error("get user name error: {}", ExceptionUtils.getFullStackTrace(e));
    }

    return email;
  }

  /**
   * 注册.
   *
   * @param ticket 参数，不明白含义
   * @param session 保存的信息
   * @return 返回视图路径
   */
  @RequestMapping("login")
  public String login(@RequestParam String ticket, HttpSession session) {
    String username = getUsername(ticket);

    if (StringUtils.isNotEmpty(username)) {
      session.setAttribute(Constants.USER_TICKET, ticket);
      session.setAttribute(Constants.USER_NAME, username);

      LOGGER.info("user {} login", username);

      return "redirect:/index";
    } else {
      return "/cas/logout";
    }
  }

  /**
   * 注销.
   *
   * @param request 请求数据
   * @param response 响应数据
   * @return 返回视图名称
   */
  @RequestMapping("logout")
  public String logout(HttpServletRequest request, HttpServletResponse response) {
    HttpSession session = request.getSession();

    String username = (String) session.getAttribute(Constants.USER_NAME);

    session.invalidate();

    LOGGER.info("user {} logout", username);

    return "redirect:" + String.format(Constants.CAS_LOGOUT, Constants.THIS_URL);
  }

}
