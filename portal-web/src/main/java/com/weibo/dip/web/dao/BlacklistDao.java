package com.weibo.dip.web.dao;

import com.weibo.dip.web.model.Searching;
import com.weibo.dip.web.model.intelligentwarning.Blacklist;

import java.util.List;
import java.util.Map;

/**
 * Created by shixi_ruibo on 2018/3/13.
 */
public interface BlacklistDao {

  /**
   * 新增一条黑名单.
   */
  Map<String, Object> create(Blacklist blacklist);

  /**
   * 根据id删除一条黑名单.
   */
  boolean delete(int id);

  /**
   * 修改一条预警策略.
   */
  long update(Blacklist blacklist);


  /**
   * 根据id查找一条黑名单.
   */
  Blacklist findBlacklistById(int id);

  /**
   * 查询符合条件的结果.
   *
   * @return 所有blacklist
   */
  List<Blacklist> searching(Searching searching, int column, String dir);

  /**
   * 查询blackName是否已存在 存在返回1，不存在返回0.
   */

  boolean isExist(String blackName);
}
