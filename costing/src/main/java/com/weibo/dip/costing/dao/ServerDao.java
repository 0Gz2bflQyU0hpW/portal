package com.weibo.dip.costing.dao;

import com.weibo.dip.costing.bean.Server;
import com.weibo.dip.costing.bean.Server.Role;
import com.weibo.dip.costing.bean.Server.Type;
import java.util.Map;
import org.springframework.dao.DataAccessException;

/**
 * Created by yurun on 18/4/23.
 */
public interface ServerDao {

  int add(Server server) throws DataAccessException;

  int delete(int id) throws DataAccessException;

  int delete(Role role, Type type, String productUuid) throws DataAccessException;

  int servers() throws DataAccessException;

  int servers(Role role) throws DataAccessException;

  Map<String, Integer> servers(Role role, Type type) throws DataAccessException;

  int servers(Role role, Type type, String productUuid) throws DataAccessException;

  int update(Server server) throws DataAccessException;

}
