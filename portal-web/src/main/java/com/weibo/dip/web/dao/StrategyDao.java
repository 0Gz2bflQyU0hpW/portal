package com.weibo.dip.web.dao;

import com.weibo.dip.web.model.Searching;
import com.weibo.dip.web.model.intelligentwarning.Strategy;

import java.util.List;
import java.util.Map;

/**
 * Created by shixi_dongxue3 on 2018/3/12.
 */
public interface StrategyDao {

  /**
   * 新增一条预警策略.
   *
   * @return 创建时间戳和id
   */
  Map<String, Object> create(Strategy strategy);

  /**
   * 根据id删除一条预警策略.
   *
   * @return 返回是否删除成功
   */
  boolean delete(int id);

  /**
   * 修改一条预警策略.
   *
   * @return 返回修改时间戳
   */
  long update(Strategy strategy);

  /**
   * 修改一条预警策略的开关.
   *
   * @return 返回修改时间戳
   */
  long updateStatus(String status, int id);

  /**
   * 根据id查找一条预警策略.
   */
  Strategy findStrategyById(int id);

  /**
   * 查询符合条件的结果.
   *
   * @return 所有strategy
   */
  List<Strategy> searching(Searching searching, int column, String dir);

  /**
   * 查询StrategyName是否已存在.
   */
  boolean isExist(String strategyName);


}
