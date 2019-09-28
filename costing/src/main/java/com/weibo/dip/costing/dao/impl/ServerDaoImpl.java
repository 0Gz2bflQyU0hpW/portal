package com.weibo.dip.costing.dao.impl;

import com.weibo.dip.costing.bean.Server;
import com.weibo.dip.costing.bean.Server.Role;
import com.weibo.dip.costing.bean.Server.Type;
import com.weibo.dip.costing.dao.ServerDao;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

/**
 * Created by yurun on 18/4/23.
 */
@Repository
public class ServerDaoImpl implements ServerDao {

  @Autowired
  private JdbcTemplate primaryJT;

  @Override
  public int add(Server server) throws DataAccessException {
    String sql = "insert into dpv2_server"
        + "(role, type, product_uuid, servers) "
        + "values(?, ?, ?, ?)";

    KeyHolder keyHolder = new GeneratedKeyHolder();

    return primaryJT.update(conn -> {
      PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

      stmt.setString(1, server.getRole().name());
      stmt.setString(2, server.getType().name());
      stmt.setString(3, server.getProductUuid());
      stmt.setInt(4, server.getServers());

      return stmt;
    }, keyHolder) == 1 ? keyHolder.getKey().intValue() : -1;
  }

  @Override
  public int delete(int id) throws DataAccessException {
    String sql = "delete from dpv2_server where id = ?";

    return primaryJT.update(sql, id);
  }

  @Override
  public int delete(Role role, Type type, String productUuid) throws DataAccessException {
    return 0;
  }

  @Override
  public int servers() throws DataAccessException {
    String sql = "select sum(servers) from dpv2_server";

    return primaryJT.queryForObject(sql, Integer.class);
  }

  @Override
  public int servers(Role role) throws DataAccessException {
    String sql = "select ifnull(sum(servers), 0) from dpv2_server where role = ?";

    return primaryJT.queryForObject(sql, Integer.class, role.name());
  }

  @Override
  public Map<String, Integer> servers(Role role, Type type) throws DataAccessException {
    String sql = "select product_uuid, sum(servers) as servers from dpv2_server"
        + " where role = ? and type = ? group by product_uuid";

    Map<String, Integer> servers = new HashMap<>();

    primaryJT.query(sql, new Object[]{role.name(), type.name()}, rs -> {
      servers.put(rs.getString("product_uuid"), rs.getInt("servers"));
    });

    return servers;
  }

  @Override
  public int servers(Role role, Type type, String productUuid) throws DataAccessException {
    String sql = "select ifnull(sum(servers), 0) from dpv2_server"
        + " where role = ? and type = ? and product_uuid = ?";

    return primaryJT.queryForObject(sql, Integer.class, role.name(), type.name(), productUuid);
  }

  @Override
  public int update(Server server) throws DataAccessException {
    String sql = "update dpv2_server set role = ?, type = ?, product_uuid = ?, servers = ?"
        + " where id = ?";

    return primaryJT.update(sql, server.getRole(), server.getType(), server.getProductUuid(),
        server.getServers(), server.getId());
  }

}
