package com.weibo.dip.web.controller.intelliwarning;

import com.weibo.dip.web.model.AnalyzeJson;
import com.weibo.dip.web.model.JedisPoolFactory;
import com.weibo.dip.web.model.intelligentwarning.Strategy;
import com.weibo.dip.web.service.StrategyService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


/**
 * Created by shixi_dongxue3 on 2018/3/12.
 */
@Controller
@RequestMapping("/intelliWarning/strategy")
public class StrategyController {

  private static final Logger LOGGER = LoggerFactory.getLogger(StrategyController.class);

  @Autowired
  private StrategyService strategyService;

  /**
   * 用于控制datatable，实现数据的显示以及排序.
   *
   * @param data 前台传入的参数，主要包含datatable中的各种设置（系统自带的）和额外的参数.
   * @return 返回String类型的参数，参数中包含查询结果和datatable的设置.
   */
  @RequestMapping(value = "/datatable", method = RequestMethod.POST)
  @ResponseBody
  public String searching(@RequestParam String data) {
    AnalyzeJson json = new AnalyzeJson(data);
    List<Strategy> list = new ArrayList<>();
    try {
      list.addAll(strategyService.searching(json));
    } catch (DataAccessException e) {
      LOGGER.error(
          "searching strategy in db error:{}",
          ExceptionUtils.getFullStackTrace(e));
    }
    return json.getResponseString(list);
  }

  /**
   * 预警策略创建.
   *
   * @param strategy 新的预警策略
   */
  @RequestMapping(value = "/create", method = RequestMethod.POST)
  @ResponseBody
  public void create(@RequestBody Strategy strategy) {
    Map<String, Object> map = new HashMap<>();
    try {
      map = strategyService.create(strategy);
      sendToRedis(String.valueOf(map.get("id")), String.valueOf(map.get("time")));
    } catch (DataAccessException e) {
      LOGGER.error(
          "create a new strategy(strategyName=" + strategy.getStrategyName() + ") to db error:{}",
          ExceptionUtils.getFullStackTrace(e));
    } catch (Exception e) {
      LOGGER.error("create id=" + map.get("id") + " in redis error:{}",
          ExceptionUtils.getFullStackTrace(e));
    }
  }

  /**
   * 预警策略修改.
   *
   * @param strategy 含有新内容的预警策略
   * @return 返回视图
   */
  @RequestMapping(value = "/update", method = RequestMethod.POST)
  @ResponseBody
  public String update(@RequestBody Strategy strategy) {
    try {
      long time = strategyService.update(strategy);
      sendToRedis(String.valueOf(strategy.getId()), String.valueOf(time));
    } catch (DataAccessException e) {
      LOGGER.error("update a strategy in db error:{}", ExceptionUtils.getFullStackTrace(e));
    } catch (Exception e) {
      LOGGER.error("update id=" + strategy.getId() + " in redis error:{}",
          ExceptionUtils.getFullStackTrace(e));
    }

    return "/intelliWarning/strategy/strategy-list";
  }

  /**
   * 预警策略修改状态.
   *
   * @param status 新状态
   * @param id     对应的预警策略id
   * @return 返回视图
   */
  @RequestMapping(value = "/updateStatus", method = RequestMethod.POST)
  @ResponseBody
  public String updateStatus(@RequestParam("status") String status, @RequestParam("id") int id) {
    try {
      long time = strategyService.updateStatus(status, id);
      sendToRedis(String.valueOf(id), String.valueOf(time));
    } catch (DataAccessException e) {
      LOGGER.error("update a strategy in db error:{}", ExceptionUtils.getFullStackTrace(e));
    } catch (Exception e) {
      LOGGER.error("update id=" + id + " in redis error:{}", ExceptionUtils.getFullStackTrace(e));
    }

    return "/intelliWarning/strategy/strategy-list";
  }

  /**
   * 根据id获取strategy详情.
   *
   * @param id 要查询的id
   * @return 详细信息
   */
  @RequestMapping("/show")
  @ResponseBody
  public Map view(@RequestParam("id") int id) {
    Map<String, Strategy> map = new HashMap<>();
    Strategy strategy = new Strategy();

    try {
      strategy = strategyService.findStrategyById(id);
    } catch (DataAccessException e) {
      LOGGER.error("get a strategy(id=" + id + ") from db error:{}",
          ExceptionUtils.getFullStackTrace(e));
    }
    map.put("strategy", strategy);

    return map;
  }

  /**
   * 预警策略删除.
   *
   * @param id 要删除的id
   * @return 返回成功删除信息
   */
  @RequestMapping("/delete")
  @ResponseBody
  public Map<String, Boolean> delete(@RequestParam("id") int id) {
    Map<String, Boolean> map = new HashMap<>();
    try {
      map.put("success", strategyService.delete(id));
      //strategyService.delete(id);
      delRedis(String.valueOf(id));
    } catch (DataAccessException e) {
      LOGGER.error("delete a strategy(id=" + id + " in db error:{}",
          ExceptionUtils.getFullStackTrace(e));
    } catch (Exception e) {
      LOGGER.error("delete id=" + id + " from redis error:{}", ExceptionUtils.getFullStackTrace(e));
    }
    return map;
  }

  /**
   * /验证strategyName是否存在.
   *
   * @param strategyName 相应的预警名称
   * @return 是否唯一 0：是 1：否
   */
  @RequestMapping("/validate")
  @ResponseBody
  public int validateStrategyName(@RequestBody @RequestParam("strategyName") String strategyName) {
    boolean result = true;

    try {
      result = strategyService.isExist(strategyName);
    } catch (DataAccessException e) {
      LOGGER.error("validate strategyName" + strategyName + " whether in db error:{}",
          ExceptionUtils.getFullStackTrace(e));
    }

    if (result) {
      return 1;
    } else {
      return 0;
    }
  }

  /**
   * 发送添加记录至redis.
   *
   * @param id   相应的id
   * @param time 修改时间
   */
  private void sendToRedis(String id, String time) {
    /*String host = "10.23.2.138";
    String password = "dipalarm";
    int timeout = 6000;
    int port = 6380;

    JedisPool jedisPool;
    JedisPoolConfig config = new JedisPoolConfig();
    config.setTestOnBorrow(true);
    jedisPool = new JedisPool(config, host, port, timeout, password);*/

    Map<String, String> map = new HashMap<String, String>();
    map.put(id, time);

    JedisPool jedisPool = JedisPoolFactory.creatJedisPool();

    Jedis jedis = null;
    try {
      jedis = jedisPool.getResource();

      jedis.hmset("update_time", map);
    } catch (Exception e) {
      LOGGER.error("error to set update_time(id=" + id + ") into redis:{}",
          ExceptionUtils.getFullStackTrace(e));
    } finally {
      if (Objects.nonNull(jedis)) {
        jedis.close();
      }
    }
  }

  /**
   * 发送删除记录至redis.
   *
   * @param id 相应的id
   */
  private void delRedis(String id) {
    /* String host = "10.23.2.138";
    String password = "dipalarm";
    int timeout = 6000;
    int port = 6380;

    JedisPool jedisPool;
    JedisPoolConfig config = new JedisPoolConfig();
    config.setTestOnBorrow(true);
    jedisPool = new JedisPool(config, host, port, timeout, password);*/

    JedisPool jedisPool = JedisPoolFactory.creatJedisPool();
    Jedis jedis = null;
    try {
      jedis = jedisPool.getResource();

      jedis.hdel("update_time", id);
    } catch (Exception e) {
      LOGGER.error("error to delete data(id=" + id + ") in redis:{}",
          ExceptionUtils.getFullStackTrace(e));
    } finally {
      if (Objects.nonNull(jedis)) {
        jedis.close();
      }
    }
  }
}

