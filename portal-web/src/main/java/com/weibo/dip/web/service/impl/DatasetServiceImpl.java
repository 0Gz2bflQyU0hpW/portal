package com.weibo.dip.web.service.impl;

import com.weibo.dip.web.dao.DatasetDao;
import com.weibo.dip.web.model.AnalyzeJson;
import com.weibo.dip.web.model.Searching;
import com.weibo.dip.web.model.datamart.Dataset;
import com.weibo.dip.web.service.DatasetService;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DatasetServiceImpl implements DatasetService {

  @Autowired
  private DatasetDao datasetDao;

  @Override
  public boolean create(Dataset dataset) {
    return this.datasetDao.create(dataset);
  }

  @Override
  public boolean delete(int id) {
    return this.datasetDao.delete(id);
  }

  @Override
  public boolean update(Dataset dataset) {
    return this.datasetDao.update(dataset);
  }

  @Override
  public Dataset findDatasetById(int id) {
    return this.datasetDao.findDatasetById(id);
  }

  @Override
  public List<Dataset> searching(AnalyzeJson json) {
    Searching searching = new Searching(json.getCondition(), json.getStarttime(), json.getEndtime(),
        json.getKeyword());
    List<Dataset> list = datasetDao.searching(searching, json.getColumn(), json.getDir());
    return list;
  }

  @Override
  public boolean isExist(String datasetName) {
    return this.datasetDao.isExist(datasetName);
  }


}