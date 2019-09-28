package com.weibo.dip.costing.service;

import com.weibo.dip.costing.bean.DipCost;
import org.springframework.dao.DataAccessException;

/**
 * Created by yurun on 18/4/24.
 */
public interface DipCostService {

  int add(DipCost dipCost) throws DataAccessException;

}
