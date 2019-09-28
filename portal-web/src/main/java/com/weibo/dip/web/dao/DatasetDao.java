package com.weibo.dip.web.dao;

import com.weibo.dip.web.model.Searching;
import com.weibo.dip.web.model.datamart.Dataset;

import java.util.List;

public interface DatasetDao {

  /**
   * 新增一个数据集.
   *
   * @return 是否创建成功
   */
  boolean create(Dataset dataset);

  /**
   * 根据id删除一个数据集.
   *
   * @return 是否删除成功
   */
  boolean delete(int id);

  /**
   * 修改一个数据集.
   *
   * @return 是否修改成功
   */
  boolean update(Dataset dataset);

  /**
   * 根据id查找一个数据集.
   */
  Dataset findDatasetById(int id);

  /**
   * 查询并分页展示.
   */
  List<Dataset> searching(Searching searching, int column, String dir);

  /**
   * 查询DatasetName是否已存在.
   */
  boolean isExist(String datasetName);

}
