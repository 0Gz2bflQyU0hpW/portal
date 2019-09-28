package com.weibo.dip.costing.service.impl;

import com.weibo.dip.costing.bean.Dataset;
import com.weibo.dip.costing.dao.ConsoleDao;
import com.weibo.dip.costing.service.ConsoleService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

/**
 * Created by yurun on 18/4/18.
 */
@Service
public class ConsoleServiceImpl implements ConsoleService {

  @Autowired
  private ConsoleDao consoleDao;

  @Override
  public List<Dataset> gets() throws DataAccessException {
    return consoleDao.gets();
  }

}
