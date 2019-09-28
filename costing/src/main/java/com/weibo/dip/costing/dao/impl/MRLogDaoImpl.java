package com.weibo.dip.costing.dao.impl;

import com.weibo.dip.costing.bean.MRLog;
import com.weibo.dip.costing.dao.MRLogDao;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/**
 * Created by yurun on 18/4/24.
 */
@Repository
public class MRLogDaoImpl implements MRLogDao {

  @Autowired
  private JdbcTemplate primaryJT;

  private static class MRLogMapper implements RowMapper<MRLog> {


    @Override
    public MRLog mapRow(ResultSet rs, int rowNum) throws SQLException {
      MRLog mrLog = new MRLog();

      mrLog.setJobid(rs.getString("jobid"));
      mrLog.setJobname(rs.getString("jobname"));
      mrLog.setHdfsBytesRead(rs.getLong("hdfs_bytes_read"));
      String catetory = rs.getString("category");
      mrLog.setCategories(Objects.nonNull(catetory) ? catetory.split("\\|") : null);
      mrLog.setCreateTime(new Date(rs.getTimestamp("c_time").getTime()));

      return mrLog;
    }

  }

  @Override
  public List<MRLog> gets(Date beginTime, Date endTime) {
    String sql = "select jobid, jobname, hdfs_bytes_read, category, c_time from dip_mr_log where c_time >= ? and c_time <= ?";

    return primaryJT.query(sql, new Object[]{beginTime, endTime}, new MRLogMapper());
  }

}
