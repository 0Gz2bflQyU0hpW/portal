package com.weibo.dip.web.controller.datamart;

import com.weibo.dip.web.model.AnalyzeJson;
import com.weibo.dip.web.model.datamart.Dataset;
import com.weibo.dip.web.service.DatasetService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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


@Controller
@RequestMapping("/datamart/dataset")
public class DatasetController {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetController.class);

  @Autowired
  private DatasetService datasetService;

  /**
   * 用于控制datatable，实现数据的显示以及排序.
   *
   * @param data 前台传入的参数，主要包含datatable中的各种设置（系统自带的）和额外的参数.
   * @return 返回String类型的参数，参数中包含查询结果和datatable的设置.
   */
  @RequestMapping(value = "/datatable", method = RequestMethod.POST)
  @ResponseBody
  public String datatable(@RequestParam String data) {
    AnalyzeJson json = new AnalyzeJson(data);
    List<Dataset> list = datasetService.searching(json);
    return json.getResponseString(list);
  }

  /**
   * 数据集创建.
   *
   * @param dataset 传入后台的数值，由Spring负责转换.
   */
  @RequestMapping(value = "/create", method = RequestMethod.POST)
  @ResponseBody
  public void create(@RequestBody Dataset dataset) {
    try {
      datasetService.create(dataset);
    } catch (DataAccessException e) {
      LOGGER.error("create a new dataset to db error:{}", ExceptionUtils.getFullStackTrace(e));
    }
  }

  /**
   * 数据集修改.
   *
   * @param dataset 修改后的Dataset用于更新.
   * @return 返回视图路径.
   */
  @RequestMapping(value = "/update", method = RequestMethod.POST)
  @ResponseBody
  public String update(@RequestBody Dataset dataset) {
    try {
      datasetService.update(dataset);
    } catch (DataAccessException e) {
      LOGGER.error("update a dataset in db error:{}", ExceptionUtils.getFullStackTrace(e));
    }
    return "/datamart/dataset/dataset-list";
  }

  /**
   * 根据id获取dataset详情.
   *
   * @param id 需要查询的id.
   * @return 返回查询结果
   */
  @RequestMapping("/show")
  @ResponseBody
  public Map<String, Dataset> view(@RequestParam("id") Integer id) {
    Map<String, Dataset> map = new HashMap<>();
    Dataset dataset;
    try {
      dataset = datasetService.findDatasetById(id);
      map.put("dataset", dataset);
    } catch (DataAccessException e) {
      LOGGER.error("get a dataset from db error:{}", ExceptionUtils.getFullStackTrace(e));
    }
    return map;
  }

  /**
   * 数据集删除.
   *
   * @param id 需要删除掉的id
   */
  @RequestMapping("/delete")
  @ResponseBody
  public void delete(@RequestParam("id") int id) {
    try {
      datasetService.delete(id);
    } catch (DataAccessException e) {
      LOGGER.error("delete a dataset in db error:{}", ExceptionUtils.getFullStackTrace(e));
    }
  }

  /**
   * 验证datasetName是否存在.
   *
   * @param datasetName 要验证的数据集名称
   * @return 返回是否存在 0：不存在  1：存在
   */
  @RequestMapping("/validate")
  @ResponseBody
  public int validateDatasetName(@RequestBody @RequestParam("datasetName") String datasetName) {
    boolean result = true;

    try {
      result = datasetService.isExist(datasetName);
    } catch (DataAccessException e) {
      LOGGER.error("validate id whether in db error:{}", ExceptionUtils.getFullStackTrace(e));
    }

    if (result) {
      return 1;
    } else {
      return 0;
    }
  }

}
