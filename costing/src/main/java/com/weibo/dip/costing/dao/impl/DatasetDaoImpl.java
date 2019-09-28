package com.weibo.dip.costing.dao.impl;

import com.weibo.dip.costing.exception.HDFSAccessException;
import com.weibo.dip.costing.bean.Dataset;
import com.weibo.dip.costing.dao.DatasetDao;
import com.weibo.dip.data.platform.commons.util.HDFSUtil;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

/**
 * Created by yurun on 18/4/19.
 */
@Repository
public class DatasetDaoImpl implements DatasetDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetDaoImpl.class);

  private static final String HDFS_RAWLOG = "hdfs.rawlog";

  @Autowired
  private JdbcTemplate primaryJT;

  @Autowired
  private Environment env;

  @Override
  public int add(Dataset dataset) throws DataAccessException {
    String sql = "insert into dpv2_category"
        + "(name, product_uuid, period, size, contacts, comment) "
        + "values(?, ?, ?, ?, ?, ?)";

    KeyHolder keyHolder = new GeneratedKeyHolder();

    return primaryJT.update(conn -> {
      PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

      stmt.setString(1, dataset.getName());
      stmt.setString(2, dataset.getProductUuid());
      stmt.setInt(3, dataset.getPeriod());
      stmt.setDouble(4, dataset.getSize());
      stmt.setString(5, StringUtils.join(dataset.getContacts(), ";"));
      stmt.setString(6, dataset.getComment());

      return stmt;
    }, keyHolder) == 1 ? keyHolder.getKey().intValue() : -1;
  }

  @Override
  public int delete(int id) throws DataAccessException {
    String sql = "delete from dpv2_category where id = ?";

    return primaryJT.update(sql, id);
  }

  @Override
  public int delete(String name) throws DataAccessException, HDFSAccessException {
    return deleteFromDB(name);
  }

  @Override
  public int deleteFromDB(String name) throws DataAccessException {
    String sql = "delete from dpv2_category where name = ?";

    return primaryJT.update(sql, name);
  }

  @Override
  public void deleteFromHDFS(String name) throws HDFSAccessException {
    try {
      HDFSUtil.deleteDir(new Path(env.getProperty(HDFS_RAWLOG), name), true);
    } catch (IOException e) {
      throw new HDFSAccessException(e.getMessage());
    }
  }

  private static class DatasetRowMapper implements RowMapper<Dataset> {

    @Override
    public Dataset mapRow(ResultSet rs, int rowNum) throws SQLException {
      Dataset dataset = new Dataset();

      dataset.setId(rs.getInt("id"));
      dataset.setName(rs.getString("name"));
      dataset.setProductUuid(rs.getString("product_uuid"));
      dataset.setPeriod(rs.getInt("period"));
      dataset.setSize(rs.getDouble("size"));
      dataset.setContacts(StringUtils.split(rs.getString("contacts"), ","));
      dataset.setComment(rs.getString("comment"));
      dataset.setLastUpdate(new Date(rs.getTime("last_update").getTime()));

      return dataset;
    }

  }

  @Override
  public Dataset get(int id) throws DataAccessException {
    String sql = "select * from dpv2_category where id = ?";

    List<Dataset> datasets = primaryJT.query(sql, new DatasetRowMapper(), id);

    return CollectionUtils.isNotEmpty(datasets) ? datasets.get(0) : null;
  }

  @Override
  public Dataset get(String name) throws DataAccessException {
    String sql = "select * from dpv2_category where name = ?";

    List<Dataset> datasets = primaryJT.query(sql, new DatasetRowMapper(), name);

    return CollectionUtils.isNotEmpty(datasets) ? datasets.get(0) : null;
  }

  @Override
  public boolean exist(String name) throws DataAccessException {
    return Objects.nonNull(get(name));
  }

  @Override
  public List<Dataset> loadFromDB() throws DataAccessException {
    String sql = "select * from dpv2_category";

    return primaryJT.query(sql, new DatasetRowMapper());
  }

  @Override
  public List<String> loadFromHDFS() throws HDFSAccessException {
    List<Path> categoryPaths;

    try {
      categoryPaths = HDFSUtil.listDirs(env.getProperty(HDFS_RAWLOG), false);
    } catch (IOException e) {
      throw new HDFSAccessException(e.getMessage());
    }

    if (CollectionUtils.isEmpty(categoryPaths)) {
      return null;
    }

    return categoryPaths.stream().map(Path::getName).collect(Collectors.toList());
  }

  @Override
  public long size() throws HDFSAccessException {
    try {
      return HDFSUtil.summary(env.getProperty(HDFS_RAWLOG)).getLength();
    } catch (IOException e) {
      throw new HDFSAccessException(e.getMessage());
    }
  }

  @Override
  public long size(String category) throws HDFSAccessException {
    try {
      ContentSummary summary = HDFSUtil.summary(new Path(env.getProperty(HDFS_RAWLOG), category));

      return Objects.nonNull(summary) ? summary.getLength() : 0L;
    } catch (IOException e) {
      throw new HDFSAccessException(e.getMessage());
    }
  }

}
