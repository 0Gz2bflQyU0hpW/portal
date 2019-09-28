package com.weibo.dip.web.service.impl;

import com.weibo.dip.web.dao.StrategyDao;
import com.weibo.dip.web.model.AnalyzeJson;
import com.weibo.dip.web.model.Searching;
import com.weibo.dip.web.model.intelligentwarning.Strategy;
import com.weibo.dip.web.service.StrategyService;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


/**
 * Created by shixi_dongxue3 on 2018/3/12.
 */
@Service
public class StrategyServiceImpl implements StrategyService {

  @Autowired
  private StrategyDao strategyDao;

  @Override
  public Map<String, Object> create(Strategy strategy) {
    return this.strategyDao.create(strategy);
  }

  @Override
  public boolean delete(int id) {
    return this.strategyDao.delete(id);
  }

  @Override
  public long update(Strategy strategy) {
    return this.strategyDao.update(strategy);
  }

  @Override
  public long updateStatus(String status, int id) {
    return this.strategyDao.updateStatus(status, id);
  }

  @Override
  public Strategy findStrategyById(int id) {
    return this.strategyDao.findStrategyById(id);
  }

  @Override
  public boolean isExist(String strategyName) {
    return this.strategyDao.isExist(strategyName);
  }

  @Override
  public List<Strategy> searching(AnalyzeJson json) {
    Searching searching = new Searching(json.getCondition(), json.getStarttime(), json.getEndtime(),
        json.getKeyword());
    List<Strategy> list = strategyDao.searching(searching, json.getColumn(), json.getDir());
    return list;
  }
}
