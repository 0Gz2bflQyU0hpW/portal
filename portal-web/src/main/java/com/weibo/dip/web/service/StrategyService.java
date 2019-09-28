package com.weibo.dip.web.service;

import com.weibo.dip.web.model.AnalyzeJson;
import com.weibo.dip.web.model.intelligentwarning.Strategy;

import java.util.List;
import java.util.Map;

/**
 * Created by shixi_dongxue3 on 2018/3/12.
 */
public interface StrategyService {

  /**
   * 新增一条预警策略.
   *
   * @param strategy 新建的strategy
   * @return 创建时间戳和id
   */
  Map<String, Object> create(Strategy strategy);

  /**
   * 根据id删除一条预警策略.
   *
   * @param id 要删除的id
   * @return 返回是否删除成功
   */
  boolean delete(int id);

  /**
   * 修改一条预警策略.
   *
   * @param strategy 修改的strategy
   * @return 返回修改时间戳
   */
  long update(Strategy strategy);

  /**
   * 修改一条预警策略的开关.
   *
   * @param status 新的状态
   * @param id 对应的策略的id
   * @return 返回修改时间戳
   */
  long updateStatus(String status, int id);

  /**
   * 根据id查找一条预警策略.
   *
   * @param id 策略id
   * @return 返回详细信息
   */
  Strategy findStrategyById(int id);

  /**
   * 查询StrategyName是否已存在.
   *
   * @param strategyName 策略名称
   * @return 存在返回1，不存在返回0
   */
  boolean isExist(String strategyName);

  /**
   * 查询符合条件的结果.
   *
   * @param json 包含搜索条件或者为排序条件的json解析
   * @return 所有符合条件的strategy
   */
  List<Strategy> searching(AnalyzeJson json);

}
