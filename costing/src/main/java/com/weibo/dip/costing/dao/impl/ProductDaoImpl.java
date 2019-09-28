package com.weibo.dip.costing.dao.impl;

import com.weibo.dip.costing.bean.Product;
import com.weibo.dip.costing.dao.ProductDao;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

/**
 * Created by yurun on 18/4/18.
 */
@Repository
public class ProductDaoImpl implements ProductDao {

  public static final Logger LOGGER = LoggerFactory.getLogger(ProductDaoImpl.class);

  @Autowired
  private JdbcTemplate primaryJT;

  public ProductDaoImpl() {
    super();
  }

  @Override
  public boolean exist(String uuid) throws DataAccessException {
    return Objects.nonNull(getByUuid(uuid)) ? true : false;
  }

  @Override
  public int add(Product product) throws DataAccessException {
    String sql = "insert into dpv2_product"
        + "(uuid, name, tech_leader, product_leader, vice_president, status, is_platform) "
        + "values(?, ?, ?, ?, ?, ?, ?)";

    KeyHolder keyHolder = new GeneratedKeyHolder();

    return primaryJT.update(conn -> {
      PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

      stmt.setString(1, product.getUuid());
      stmt.setString(2, product.getName());
      stmt.setString(3, product.getTechLeader());
      stmt.setString(4, product.getProductLeader());
      stmt.setString(5, product.getVicePresident());
      stmt.setString(6, product.getStatus());
      stmt.setString(7, product.isPlatform() ? "1" : "0");

      return stmt;
    }, keyHolder) == 1 ? keyHolder.getKey().intValue() : -1;
  }

  @Override
  public int delete(int id) throws DataAccessException {
    String sql = "delete from dpv2_product where id = ?";

    return primaryJT.update(sql, id);
  }

  private static class ProductRowMapper implements RowMapper<Product> {

    @Override
    public Product mapRow(ResultSet rs, int rowNum) throws SQLException {
      Product product = new Product();

      product.setId(rs.getInt("id"));
      product.setUuid(rs.getString("uuid"));
      product.setName(rs.getString("name"));
      product.setTechLeader(rs.getString("tech_leader"));
      product.setProductLeader(rs.getString("product_leader"));
      product.setStatus(rs.getString("status"));

      if (rs.getString("is_platform").equals("1")) {
        product.setPlatform(true);
      }

      product.setLastUpdate(new Date(rs.getTimestamp("last_update").getTime()));

      return product;
    }

  }

  @Override
  public Product get(int id) throws DataAccessException {
    String sql = "select * from dpv2_product where id = ?";

    List<Product> products = primaryJT.query(sql, new ProductRowMapper(), id);

    return CollectionUtils.isNotEmpty(products) ? products.get(0) : null;
  }

  @Override
  public Product getByUuid(String uuid) throws DataAccessException {
    String sql = "select * from dpv2_product where uuid = ?";

    List<Product> products = primaryJT.query(sql, new ProductRowMapper(), uuid);

    return CollectionUtils.isNotEmpty(products) ? products.get(0) : null;
  }

  @Override
  public Product getByName(String name) throws DataAccessException {
    String sql = "select * from dpv2_product where name = ?";

    List<Product> products = primaryJT.query(sql, new ProductRowMapper(), name);

    return CollectionUtils.isNotEmpty(products) ? products.get(0) : null;
  }

  @Override
  public List<Product> gets() throws DataAccessException {
    String sql = "select * from dpv2_product";

    return primaryJT.query(sql, new ProductRowMapper());
  }

  @Override
  public int update(Product product) throws DataAccessException {
    String sql = "update dpv2_product set "
        + "name = ?, tech_leader = ?, product_leader = ?, "
        + "vice_president = ?, status = ?, is_platform = ? where uuid = ?";

    return primaryJT
        .update(sql, product.getName(), product.getTechLeader(), product.getProductLeader(),
            product.getVicePresident(), product.getStatus(), product.isPlatform() ? "1" : "0",
            product.getUuid());
  }

}
