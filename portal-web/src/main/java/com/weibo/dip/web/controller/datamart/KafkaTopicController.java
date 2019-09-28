package com.weibo.dip.web.controller.datamart;

import com.weibo.dip.web.model.AnalyzeJson;
import com.weibo.dip.web.model.datamart.KafkaTopic;
import com.weibo.dip.web.service.KafkaTopicService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Created by shixi_dongxue3 on 2018/1/31.
 */
@Controller
@RequestMapping("/datamart/kafka/topic")
public class KafkaTopicController {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopicController.class);

  @Autowired
  private KafkaTopicService kafkaTopicService;

  /**
   * 用于实现datatable的显示，以及数据的排序.
   *
   * @param data 前台传入的参数，主要包含datatable中的各种设置（系统自带的）和额外的参数.
   * @return 返回String类型的参数，参数中包含查询结果和datatable的设置.
   */
  @RequestMapping(value = "/datatable", method = RequestMethod.POST)
  @ResponseBody
  public String datatable(@RequestParam String data) {
    AnalyzeJson json = new AnalyzeJson(data);
    List<KafkaTopic> list = kafkaTopicService.searching(json);
    return json.getResponseString(list);
  }

  /**
   * topic创建.
   *
   * @param kafkaTopic 要创建的变量
   */
  @RequestMapping(value = "/create", method = RequestMethod.POST)
  @ResponseBody
  public void create(@RequestBody KafkaTopic kafkaTopic) {
    Integer topicNum = null;
    try {
      topicNum = kafkaTopicService.isUnique(kafkaTopic.getTopicName());
    } catch (Exception e) {
      LOGGER.error("get number of topicName error:{}", ExceptionUtils.getFullStackTrace(e));
    }
    if (topicNum == 1) {
      LOGGER.error("topic is already exist！");
    }

    kafkaTopicService.create(kafkaTopic);
  }

  /**
   * topic修改.
   *
   * @param kafkaTopic 用于更新的数据
   * @return 返回视图路径
   */
  @RequestMapping(value = "/update", method = RequestMethod.POST)
  @ResponseBody
  public String update(@RequestBody KafkaTopic kafkaTopic) {
    try {
      kafkaTopicService.update(kafkaTopic);
    } catch (Exception e) {
      LOGGER.error("update topic error:{}", ExceptionUtils.getFullStackTrace(e));
    }
    return "/datamart/kafka/topic/topic-list";
  }

  /**
   * 根据id获取topic详情.
   *
   * @param id 要查的id
   * @return 返回详细信息
   */
  @RequestMapping("/show")
  @ResponseBody
  public Map view(@RequestParam("id") Integer id) {
    Map map = new HashMap();
    KafkaTopic kafkaTopic = new KafkaTopic();

    try {
      kafkaTopic = kafkaTopicService.findKafkaTopicById(id);
    } catch (Exception e) {
      LOGGER.error("get topic by id error:{}", ExceptionUtils.getFullStackTrace(e));
    }
    map.put("kafkaTopic", kafkaTopic);
    return map;
  }

  /**
   * topic删除.
   *
   * @param id 要删除的id
   */
  @RequestMapping("/delete")
  @ResponseBody
  public void delete(@RequestParam("id") Integer id) {
    try {
      kafkaTopicService.delete(id);
    } catch (Exception e) {
      LOGGER.error("delete topic by id error:{}", ExceptionUtils.getFullStackTrace(e));
    }
  }

  /**
   * 验证topicName的唯一性.
   *
   * @param topicName 名称
   * @return 返回是否存在 0：不存在 1 ：存在
   */
  @RequestMapping("/validate")
  @ResponseBody
  public Integer validateKafkaTopicName(@RequestBody @RequestParam("topicName") String topicName) {
    Integer topicNum = null;

    try {
      topicNum = kafkaTopicService.isUnique(topicName);
    } catch (Exception e) {
      LOGGER.error("get number of topicName error:{}", ExceptionUtils.getFullStackTrace(e));
    }

    return topicNum;
  }

}
