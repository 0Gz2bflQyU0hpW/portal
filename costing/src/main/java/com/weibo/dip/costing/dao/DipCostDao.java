package com.weibo.dip.costing.dao;

import com.weibo.dip.costing.bean.DipCost;
import org.springframework.dao.DataAccessException;

/**
 * Created by yurun on 18/4/24.
 */
public interface DipCostDao {

  int add(DipCost dipCost) throws DataAccessException;

}
