package com.weibo.dip.web.controller.datamart;

import com.weibo.dip.web.model.AnalyzeJson;
import com.weibo.dip.web.model.datamart.KafkaConsumer;
import com.weibo.dip.web.service.KafkaConsumerService;

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
@RequestMapping("/datamart/kafka/consumer")
public class KafkaConsumerController {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerController.class);

  @Autowired
  private KafkaConsumerService kafkaConsumerService;

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
    List<KafkaConsumer> list = kafkaConsumerService.searching(json);
    return json.getResponseString(list);
  }

  /**
   * consumer创建.
   *
   * @param kafkaConsumer 传入参数，由Spring初始化
   */
  @RequestMapping(value = "/create", method = RequestMethod.POST)
  @ResponseBody
  public void create(@RequestBody KafkaConsumer kafkaConsumer) {
    Integer consumerNum = null;
    try {
      consumerNum = kafkaConsumerService
          .isUnique(kafkaConsumer.getTopicName(), kafkaConsumer.getConsumerGroup());
    } catch (Exception e) {
      LOGGER.error("get the number topicName and consumerGroup error:{}",
          ExceptionUtils.getFullStackTrace(e));
    }
    if (consumerNum == 1) {
      LOGGER.error("the consumer is already exist！");
    }

    kafkaConsumerService.create(kafkaConsumer);
  }

  /**
   * consumer修改.
   *
   * @param kafkaConsumer 用于更新的数据
   * @return 返回视图路径
   */
  @RequestMapping(value = "/update", method = RequestMethod.POST)
  @ResponseBody
  public String update(@RequestBody KafkaConsumer kafkaConsumer) {
    try {
      kafkaConsumerService.update(kafkaConsumer);
    } catch (Exception e) {
      LOGGER.error("update consumer error:{}", ExceptionUtils.getFullStackTrace(e));
    }

    return "/datamart/kafka/consumer/consumer-list";
  }

  /**
   * 根据id获取consumer详情.
   *
   * @param id 要查的参数id
   * @return 返回详细信息
   */
  @RequestMapping("/show")
  @ResponseBody
  public Map view(@RequestParam("id") Integer id) {
    Map map = new HashMap();
    KafkaConsumer kafkaConsumer = new KafkaConsumer();

    try {
      kafkaConsumer = kafkaConsumerService.findKafkaConsumerById(id);
    } catch (Exception e) {
      LOGGER.error("get consumer by id error:{}", ExceptionUtils.getFullStackTrace(e));
    }

    map.put("kafkaConsumer", kafkaConsumer);

    return map;
  }

  /**
   * consumer删除.
   *
   * @param id 要删除的id
   */
  @RequestMapping("/delete")
  @ResponseBody
  public void delete(@RequestParam("id") Integer id) {
    try {
      kafkaConsumerService.delete(id);
    } catch (Exception e) {
      LOGGER.error("delete consumer by id error:{}", ExceptionUtils.getFullStackTrace(e));
    }
  }

  /**
   * 验证consumerGroup的唯一性.
   *
   * @param topicName 名称
   * @param consumerGroup 所属消费组
   * @return 返回是否存在 0：不存在 1 ：存在
   */
  @RequestMapping("/validate")
  @ResponseBody
  public Integer validateKafkaConsumerGroup(@RequestParam("topicName") String topicName,
      @RequestParam("consumerGroup") String consumerGroup) {
    Integer consumerNum = null;

    try {
      consumerNum = kafkaConsumerService.isUnique(topicName, consumerGroup);
    } catch (Exception e) {
      LOGGER.error("get the number of topicName and consumerGroup:{}",
          ExceptionUtils.getFullStackTrace(e));
    }

    return consumerNum;
  }

}
