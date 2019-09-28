package com.weibo.dip.web.dao.impl;

import com.weibo.dip.web.common.DatasetRowMapper;
import com.weibo.dip.web.dao.DatasetDao;
import com.weibo.dip.web.model.Searching;
import com.weibo.dip.web.model.datamart.Dataset;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.stereotype.Repository;

@Repository("datasetDao")
public class DatasetDaoImpl implements DatasetDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetDaoImpl.class);

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Override
  public boolean create(Dataset dataset) {
    if (isExist(dataset.getDatasetName())) {
      LOGGER.error("the datasetName already exists！");
      return false;
    } else {
      jdbcTemplate.update(
          "INSERT INTO dataset(id, "
              + "dataset_name, "
              + "product, "
              + "create_time, "
              + "update_time, "
              + "store_period, "
              + "size, "
              + "contact_person, "
              + "comment) "
              + "VALUES(NULL,?,?,NULL,NULL,?,?,?,?)",
          new PreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps) throws SQLException {
              ps.setString(1, dataset.getDatasetName());
              ps.setString(2, dataset.getProduct());
              ps.setInt(3, dataset.getStorePeriod());
              ps.setFloat(4, dataset.getSize());
              ps.setString(5, dataset.getContactPerson());
              if (StringUtils.isBlank(dataset.getComment())) {
                ps.setString(6, null);
              } else {
                ps.setString(6, dataset.getComment());
              }
            }
          });
      return true;
    }
  }

  @Override
  public boolean delete(int id) {
    String sql = "delete from dataset where id=?";
    int result = jdbcTemplate.update(sql, id);

    if (result == 1) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean update(Dataset dataset) {
    int result = jdbcTemplate.update(
        "UPDATE dataset SET "
            + "dataset_name =?, product=?, store_period=?, size=?, contact_person=?, comment=? "
            + "WHERE id=?",
        new PreparedStatementSetter() {
          @Override
          public void setValues(PreparedStatement ps) throws SQLException {
            ps.setString(1, dataset.getDatasetName());
            ps.setString(2, dataset.getProduct());
            ps.setInt(3, dataset.getStorePeriod());
            ps.setFloat(4, dataset.getSize());
            ps.setString(5, dataset.getContactPerson());
            if (dataset.getComment().isEmpty()) {
              ps.setString(6, null);
            } else {
              ps.setString(6, dataset.getComment());
            }
            ps.setInt(7, dataset.getId());
          }
        });

    if (result == 1) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Dataset findDatasetById(int id) {
    return jdbcTemplate.queryForObject("select * from dataset where id=?", new Object[] {id},
        new DatasetRowMapper());
  }

  @Override
  public List<Dataset> searching(Searching searching, int column, String dir) {
    String[] line = {"id", "dataset_name", "product", "update_time", "store_period", "size"};
    List<Dataset> list = null;
    String sql;
    if (searching.isExit()) {
      sql = "select * from dataset where " + searching.getCondition() + " like '%" + searching
          .getKeyword() + "%' and create_time between '" + searching.getStarttime() + "' and '"
          + searching.getEndtime() + "' order by " + line[column] + " " + dir;

    } else {
      sql = "select * from dataset order by " + line[column] + " " + dir;
    }
    list = jdbcTemplate.query(sql, new DatasetRowMapper());
    return list;
  }

  @Override
  public boolean isExist(String datasetName) {
    String sql = "select count(*) from dataset where dataset_name='" + datasetName + "'";
    int resule = jdbcTemplate.queryForObject(sql, Integer.class);

    if (resule == 1) {
      return true;
    } else {
      return false;
    }
  }
}