package com.weibo.dip.costing.service.impl;

import com.weibo.dip.costing.bean.Server;
import com.weibo.dip.costing.bean.Server.Role;
import com.weibo.dip.costing.bean.Server.Type;
import com.weibo.dip.costing.dao.ServerDao;
import com.weibo.dip.costing.service.ServerService;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

/**
 * Created by yurun on 18/4/23.
 */
@Service
public class ServerServiceImpl implements ServerService {

  @Autowired
  private ServerDao serverDao;

  @Override
  public int add(Server server) throws DataAccessException {
    return serverDao.add(server);
  }

  @Override
  public int delete(int id) throws DataAccessException {
    return serverDao.delete(id);
  }

  @Override
  public int delete(Role role, Type type, String productUuid) throws DataAccessException {
    return serverDao.delete(role, type, productUuid);
  }

  @Override
  public int servers() throws DataAccessException {
    return serverDao.servers();
  }

  @Override
  public Map<String, Integer> servers(Role role, Type type) throws DataAccessException {
    return serverDao.servers(role, type);
  }

  @Override
  public int servers(Role role, Type type, String productUuid) throws DataAccessException {
    return serverDao.servers(role, type, productUuid);
  }

  @Override
  public int update(Server server) throws DataAccessException {
    return serverDao.update(server);
  }

}
