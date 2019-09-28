package com.weibo.dip.data.platform.services.server.dao.impl;

import com.weib.dip.data.platform.services.client.model.Dataset;
import com.weibo.dip.data.platform.services.server.dao.DatasetDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by ftx on 2016/12/28.
 */
@Repository
public class DatasetDaoImpl implements DatasetDao{

    @Autowired
    private JdbcTemplate jdbcTemplate;
    private RowMapper<Dataset> datasetRowMapper = new RowMapper<Dataset>() {
        @Override
        public Dataset mapRow(ResultSet rs, int rowNum) throws SQLException {
            Dataset dataset = new Dataset();
            dataset.setId(rs.getInt("id"));
            dataset.setName(rs.getString("name"));
            dataset.setProduct(rs.getString("product"));
            dataset.setDepartment(rs.getString("department"));
            dataset.setType(rs.getString("type"));
            dataset.setSize(rs.getLong("size"));
            dataset.setSaveTime(rs.getLong("save_time"));
            dataset.setContacts(rs.getString("contacts").split(";"));
            dataset.setSaveStrategy(rs.getString("save_strategy"));
            dataset.setComment(rs.getString("comment"));
            dataset.setCreateTtime(rs.getDate("create_time"));
            dataset.setUpdateTime(rs.getDate("update_time"));
            return dataset;
        }
    };

    @Override
    public List<Dataset> datasetList() throws SQLException{
        String sql = "select * from dataset order by id";
        return jdbcTemplate.query(sql, datasetRowMapper);
    }

    @Override
    public List<String> datasetNameList() throws SQLException {
        String sql="select name from dataset";
        return jdbcTemplate.queryForList(sql,String.class);
    }
}
