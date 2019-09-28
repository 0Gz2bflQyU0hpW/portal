package com.weibo.dip.web.common;

import com.weibo.dip.web.model.datamart.Dataset;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class DatasetRowMapper implements RowMapper<Dataset> {

  @Override
  public Dataset mapRow(ResultSet rs, int rowNum) throws SQLException {
    Dataset dataset = new Dataset();

    dataset.setId(rs.getInt("id"));
    dataset.setDatasetName(rs.getString("dataset_name"));
    dataset.setProduct(rs.getString("product"));
    dataset.setCreateTime(rs.getTimestamp("create_time"));
    dataset.setUpdateTime(rs.getTimestamp("update_time"));
    dataset.setStorePeriod(rs.getInt("store_period"));
    dataset.setSize(rs.getFloat("size"));
    dataset.setContactPerson(rs.getString("contact_person"));
    rs.getString("comment");
    if (!rs.wasNull()) {
      dataset.setComment(rs.getString("comment"));
    }

    return dataset;
  }
}
