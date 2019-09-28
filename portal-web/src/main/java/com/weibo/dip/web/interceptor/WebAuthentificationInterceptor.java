package com.weibo.dip.web.interceptor;

import com.weibo.dip.web.common.Constants;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

public class WebAuthentificationInterceptor implements HandlerInterceptor {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(WebAuthentificationInterceptor.class);

  @Override
  public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
      throws Exception {
    String userTicket = (String) request.getSession().getAttribute(Constants.USER_TICKET);

    if (StringUtils.isEmpty(userTicket)) {
      response.sendRedirect(String.format(Constants.CAS_LOGIN, Constants.THIS_URL));

      return false;
    }

    return true;
  }

  public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler,
      ModelAndView modelAndView) throws Exception {
    //LOGGER.info("postHandle");
  }

  public void afterCompletion(HttpServletRequest request, HttpServletResponse response,
      Object handler,
      Exception ex) throws Exception {
  }

}
