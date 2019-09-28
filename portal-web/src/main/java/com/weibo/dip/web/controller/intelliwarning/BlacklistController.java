package com.weibo.dip.web.controller.intelliwarning;

import com.weibo.dip.web.model.AnalyzeJson;
import com.weibo.dip.web.model.JedisPoolFactory;
import com.weibo.dip.web.model.intelligentwarning.Blacklist;
import com.weibo.dip.web.service.BlacklistService;

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


@Controller
@RequestMapping("/intelliWarning/blacklist")
public class BlacklistController {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlacklistController.class);

  @Autowired
  private BlacklistService blacklistService;

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
    List<Blacklist> list = new ArrayList<>();
    try {
      list.addAll(blacklistService.searching(json));
    } catch (DataAccessException e) {
      LOGGER.error(
          "searching blacklist in db error:{}",
          ExceptionUtils.getFullStackTrace(e));
    }
    return json.getResponseString(list);
  }

  /**
   * 添加黑名单.
   *
   * @param blacklist 传入后台的数值，由Spring负责转换.
   */
  @RequestMapping(value = "/create", method = RequestMethod.POST)
  @ResponseBody
  public void create(@RequestBody Blacklist blacklist) {
    Map<String, Object> map = new HashMap<>();
    try {
      map = blacklistService.create(blacklist);
      setJedis(String.valueOf(map.get("id")), String.valueOf(map.get("time")));
    } catch (DataAccessException e) {
      LOGGER.error(
          "create a new blacklist(blackName=" + blacklist.getBlackName() + ") to db error:{}",
          ExceptionUtils.getFullStackTrace(e));
    } catch (Exception e) {
      LOGGER.error("create id=" + map.get("id") + " in redis error:{}",
          ExceptionUtils.getFullStackTrace(e));
    }
  }

  /**
   * 更新黑名单.
   *
   * @param blacklist 新的黑名单
   * @return 返回视图名称
   */
  @RequestMapping(value = "/update", method = RequestMethod.POST)
  @ResponseBody
  public String update(@RequestBody Blacklist blacklist) {

    try {
      long time = blacklistService.update(blacklist);
      setJedis(String.valueOf(blacklist.getId()), String.valueOf(time));
    } catch (DataAccessException e) {
      LOGGER.error("update a blacklist in db error:{}", ExceptionUtils.getFullStackTrace(e));
    } catch (Exception e) {
      LOGGER.error("update id=" + blacklist.getId() + " in redis error:{}",
          ExceptionUtils.getFullStackTrace(e));
    }

    return "/intelliWarning/blacklist/blacklist-list";
  }

  /**
   * 根据id获取blacklist详情.
   *
   * @param id 需要查询的id
   * @return 返回详细信息
   */
  @RequestMapping("/show")
  @ResponseBody
  public Map view(@RequestParam("id") Integer id) {
    Map<String, Blacklist> map = new HashMap<>();
    Blacklist blacklist = new Blacklist();

    try {
      blacklist = blacklistService.findBlacklistById(id);
    } catch (DataAccessException e) {
      LOGGER.error("get a blacklist(id=" + id + ") from db error:{}",
          ExceptionUtils.getFullStackTrace(e));
    }
    map.put("blacklist", blacklist);

    return map;
  }

  /**
   * 删除黑名单.
   *
   * @param id 要删除的黑名单is
   * @return 返回删除成功信息
   */
  @RequestMapping("/delete")
  @ResponseBody
  public Map<String, Boolean> delete(@RequestParam("id") int id) {
    Map<String, Boolean> map = new HashMap<>();

    try {
      map.put("success", blacklistService.delete(id));
      deleteJedis(String.valueOf(id));
      /* blacklistService.delete(id);*/
    } catch (DataAccessException e) {
      LOGGER.error("delete a blacklist(id=" + id + " in db error:{}",
          ExceptionUtils.getFullStackTrace(e));
    } catch (Exception e) {
      LOGGER.error("delete id=" + id + " from redis error:{}", ExceptionUtils.getFullStackTrace(e));
    }

    return map;
  }

  /**
   * 验证blackName的唯一性.
   *
   * @param blackName 要验证的名称
   * @return 返回是否唯一 0：是 1 ：否
   */
  @RequestMapping("/validate")
  @ResponseBody
  public Integer validateBlackName(@RequestBody @RequestParam("blackName") String blackName) {
    boolean blackNum = true;

    try {
      blackNum = blacklistService.isExist(blackName);
    } catch (DataAccessException e) {
      LOGGER.error("validate blackName" + blackName + " whether in db error:{}",
          ExceptionUtils.getFullStackTrace(e));
    }

    if (blackNum) {
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
  public void setJedis(String id, String time) {
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

      jedis.hmset("blacklistupdate_time", map);
    } catch (Exception e) {
      LOGGER.error("error to set blacklistupdate_time(id=" + id + ") into redis:{}",
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
  public void deleteJedis(String id) {
    /*String host = "10.23.2.138";
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

      jedis.hdel("blacklistupdate_time", id);
    } catch (Exception e) {
      LOGGER.error("error to set blacklistupdate_time(id=" + id + ") into redis:{}",
          ExceptionUtils.getFullStackTrace(e));
    } finally {
      if (Objects.nonNull(jedis)) {
        jedis.close();
      }
    }
  }

}