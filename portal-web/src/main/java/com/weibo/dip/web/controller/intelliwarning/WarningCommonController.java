package com.weibo.dip.web.controller.intelliwarning;

import com.weibo.dip.web.service.WarningCommonService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/** Created by haisen on 2018/5/18. */
@Controller
public class WarningCommonController {

  @Autowired WarningCommonService warningCommonService;

  /**
   * 获取bussiness列表.
   *
   * @return 返回bussiness列表
   */
  @RequestMapping("/listBusiness")
  @ResponseBody
  public Map<String, List<String>> listBusiness() {
    Map<String, List<String>> map = new HashMap<>();
    map.put("businessList", warningCommonService.getAllBusiness());
    return map;
  }

  /**
   * 获取某个business下的dimensions和metrics.
   *
   * @param business 指定业务名称
   * @return dimensions和metrics的详细信息
   */
  @RequestMapping("/getDimensionsAndMetrics")
  @ResponseBody
  public Map<String, Set<String>> getDimensionsAndMetrics(
      @RequestParam("business") String business) {
    Map<String, Set<String>> map = new HashMap<>();
    map.put("dimensions", warningCommonService.getDimensionsBybusiness(business));
    map.put("metrics", warningCommonService.getMetricsBybusiness(business));
    return map;
  }

  /**
   * 获取某个business下的dimensions.
   *
   * @param business 指定业务名称
   * @return dimensions的详细信息
   */
  @RequestMapping("/getDimensions")
  @ResponseBody
  public Map<String, Set<String>> getDimensions(@RequestParam("business") String business) {
    Map<String, Set<String>> map = new HashMap<>();
    map.put("dimensions", warningCommonService.getDimensionsBybusiness(business));
    return map;
  }
}
