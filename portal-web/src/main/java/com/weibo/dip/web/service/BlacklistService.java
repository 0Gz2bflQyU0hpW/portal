package com.weibo.dip.web.service;

import com.weibo.dip.web.model.AnalyzeJson;
import com.weibo.dip.web.model.intelligentwarning.Blacklist;

import java.util.List;
import java.util.Map;

/**
 * Created by shixi_ruibo on 2018/3/13.
 */
public interface BlacklistService {

  /**
   * 新增一个黑名单.
   *
   * @param blacklist 新的黑名单
   * @return 创建时间戳和id
   */
  Map<String, Object> create(Blacklist blacklist);

  /**
   * 根据id删除一个黑名单.
   *
   * @param id 要删除的id
   * @return 返回是否删除成功
   */
  boolean delete(int id);

  /**
   * 修改一个黑名单.
   *
   * @param blacklist 要更新的黑名单
   * @return 返回修改时间戳
   */
  long update(Blacklist blacklist);

  /**
   * 根据id查找一位黑名单.
   *
   * @param id 要查询的id
   * @return 返回信息
   */
  Blacklist findBlacklistById(int id);

  /**
   * 查询blackName是否已存在.
   *
   * @param blackName 黑名单名称
   * @return 是否存在
   */
  boolean isExist(String blackName);

  /**
   * 查询符合条件的结果.
   *
   * @param json 包含搜索条件或者为排序条件的json解析
   * @return 所有查询到的blacklist
   */
  List<Blacklist> searching(AnalyzeJson json);

}
