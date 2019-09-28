package com.weibo.dip.web.service;

import com.weibo.dip.web.model.AnalyzeJson;
import com.weibo.dip.web.model.datamart.Dataset;

import java.util.List;

public interface DatasetService {

  /**
   * 新增一个数据集.
   *
   * @param dataset 新建的数据集
   * @return 是否创建成功
   */
  boolean create(Dataset dataset);

  /**
   * 根据id删除一个数据集.
   *
   * @param id 要删除的id
   * @return 是否删除成功
   */
  boolean delete(int id);

  /**
   * 修改一个数据集.
   *
   * @param dataset 更新的数据集
   * @return 是否修改成功
   */
  boolean update(Dataset dataset);

  /**
   * 根据id查找一个数据集.
   *
   * @param id 要查询的id
   * @return 返回详细信息
   */
  Dataset findDatasetById(int id);

  /**
   * 查询符合条件的结果.
   *
   * @param json 包含搜索条件或者为排序条件的json解析
   * @return 所有查询到的数据集
   */
  List<Dataset> searching(AnalyzeJson json);

  /**
   * 查询DatasetName是否已存在.
   *
   * @param datasetName 数据集名称
   * @return 是否存在
   */
  boolean isExist(String datasetName);


}
