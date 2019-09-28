package com.weibo.dip.web.common;

import com.weibo.dip.web.model.Metadata;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.jdbc.core.RowMapper;

/**
 * Created by haisen on 2018/4/26.
 */
public class MetadataRowMapper implements RowMapper<Metadata> {

  //for test

  @Override
  public Metadata mapRow(ResultSet rs, int rowNum) throws SQLException {
    Metadata metadata = new Metadata();

    metadata.setId(rs.getInt("id"));
    metadata.setBusiness(rs.getString("business"));
    String dim = rs.getString("dimensions");
    String met = rs.getString("metrics");
    String[] dimensions = dim.split(",");
    String[] metrics = met.split(",");
    List<String> dimensionsstr = java.util.Arrays.asList(dimensions);
    List<String> metricsstr = java.util.Arrays.asList(metrics);

    metadata.setDimension(dimensionsstr);
    metadata.setMetrics(metricsstr);
    return metadata;

  }
}
