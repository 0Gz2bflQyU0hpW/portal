package com.weibo.dip.portal.example.dao.impl;

import com.weibo.dip.portal.example.dao.DatasetDao;
import com.weibo.dip.portal.example.model.Dataset;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.util.List;

@Repository("datasetDao")
public class DatasetDaoImpl implements DatasetDao {

	private static final Logger LOGGER = LoggerFactory.getLogger(DatasetDaoImpl.class);

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Override
	public int create(Dataset dataset) {
		int id = 0;

		try {
			KeyHolder keyHolder = new GeneratedKeyHolder();

			int affected = jdbcTemplate.update(new PreparedStatementCreator() {

				@Override
				public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
					PreparedStatement pstmt = conn.prepareStatement("insert into datasets (name) values (?)",
							Statement.RETURN_GENERATED_KEYS);

//					pstmt.setString(1, dataset.getName());

					return pstmt;
				}

			}, keyHolder);

			if (affected == 1) {
				id = keyHolder.getKey().intValue();
			}
		} catch (DataAccessException e) {
			LOGGER.error("DatasetDaoImpl create error: " + ExceptionUtils.getFullStackTrace(e));
		}

		return id;
	}

	public static class DatasetRowMapper implements RowMapper<Dataset> {

		@Override
		public Dataset mapRow(ResultSet rs, int rowNum) throws SQLException {
			Dataset dataset = new Dataset();

			dataset.setId(rs.getInt("id"));
			dataset.setName(rs.getString("name"));

			return dataset;
		}

	}

	@Override
	public Dataset read(int id) {
		List<Dataset> datasets = null;

		try {
			datasets = jdbcTemplate.query("select * from datasets where id = ?", new DatasetRowMapper(), id);
		} catch (DataAccessException e) {
			LOGGER.error("DatasetDaoImpl read error: " + ExceptionUtils.getFullStackTrace(e));
		}

		return CollectionUtils.isNotEmpty(datasets) ? datasets.get(0) : null;
	}

	@Override
	public int update(Dataset dataset) {
		int affected = 0;

		try {
			affected = jdbcTemplate.update("update datasets set name = ? where id = ?", dataset.getName(),
					dataset.getId());
		} catch (DataAccessException e) {
			LOGGER.error("DatasetDaoImpl update error: " + ExceptionUtils.getFullStackTrace(e));
		}

		return affected;
	}

	@Override
	public int delete(int id) {
		int affected = 0;

		try {
			affected = jdbcTemplate.update("delete from datasets where id = ?", id);
		} catch (DataAccessException e) {
			LOGGER.error("DatasetDaoImpl update error: " + ExceptionUtils.getFullStackTrace(e));
		}

		return affected;
	}

}
