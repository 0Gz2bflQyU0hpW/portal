package com.weibo.dip.web.controller;

import com.weibo.dip.web.common.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpSession;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class NavController {

  private static final Logger LOGGER = LoggerFactory.getLogger(NavController.class);

  @Autowired private JdbcTemplate jdbcTemplate;

  @RequestMapping("/")
  public String portal() {
    return "index";
  }

  @RequestMapping("/index")
  public String list() {
    return "index";
  }

  /**
   * 得到Session.
   *
   * @param session 前台存的数据
   * @return 返回username
   */
  @RequestMapping("/getSession")
  @ResponseBody
  public Map<String, Object> getSession(HttpSession session) {
    Map<String, Object> map = new HashMap<>();
    map.put("username", session.getAttribute(Constants.USER_NAME));

    return map;
  }

  /**
   * 从数据库中取出product列表.
   *
   * @return product列表
   */
  @RequestMapping("/listProduct")
  @ResponseBody
  public Map<String, List<String>> listProduct() {
    Map<String, List<String>> map = new HashMap<>();
    List<String> productList = null;
    String sql = "select product_name from dataplatform_product";

    try {
      productList = jdbcTemplate.queryForList(sql, String.class);
    } catch (Exception e) {
      LOGGER.error("get products error:{}", ExceptionUtils.getFullStackTrace(e));
    }
    map.put("productList", productList);
    return map;
  }

  /**
   * 获取集群下拉菜单.
   *
   * @return 下拉菜单的详细内容
   */
  @RequestMapping("/listCluster")
  @ResponseBody
  public Map<String, List<String>> listCluster() {

    List<String> clusterList = new ArrayList<>();
    // 最后完成从数据库中读取,类似ListProduct()，还为规定数据库
    clusterList.add("k1001");
    clusterList.add("k1002");
    clusterList.add("k1003");
    Map<String, List<String>> map = new HashMap<>();
    map.put("clusterList", clusterList);
    return map;
  }
}
