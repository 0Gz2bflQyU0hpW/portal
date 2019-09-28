package com.weibo.dip.costing.service.impl;

import com.weibo.dip.costing.bean.DipCost;
import com.weibo.dip.costing.dao.DipCostDao;
import com.weibo.dip.costing.service.DipCostService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

/**
 * Created by yurun on 18/4/24.
 */
@Service
public class DipCostServiceImpl implements DipCostService {

  @Autowired
  private DipCostDao dipCostDao;

  @Override
  public int add(DipCost dipCost) throws DataAccessException {
    return dipCostDao.add(dipCost);
  }

}
