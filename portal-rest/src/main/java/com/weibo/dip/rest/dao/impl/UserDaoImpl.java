package com.weibo.dip.rest.dao.impl;

import com.weibo.dip.rest.bean.User;
import com.weibo.dip.rest.dao.UserDao;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by yurun on 17/10/20.
 */
@Repository
public class UserDaoImpl implements UserDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserDaoImpl.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static class UserRowMapper implements RowMapper<User> {

        public User mapRow(ResultSet rs, int rowNum) throws SQLException {
            User user = new User();

            user.setUserId(rs.getString("userId"));
            user.setUserName(rs.getString("userName"));
            user.setRole(rs.getString("role"));
            user.setTrueName(rs.getString("trueName"));
            user.setSex(rs.getString("sex"));
            user.setDept(rs.getString("dept"));
            user.setPosition(rs.getString("position"));
            user.setMobile(rs.getString("mobile"));
            user.setEmail(rs.getString("email"));
            user.setNoticeType(rs.getString("noticeType"));
            user.setNoticeMethod(rs.getString("noticeMethod"));
            user.setNoticeEmail(rs.getString("noticeEmail"));
            user.setNoticeMobile(rs.getString("noticeMobile"));
            user.setPassword(rs.getString("password"));
            user.setAccesskey(rs.getString("accesskey"));
            user.setDepartlevel(rs.getInt("departlevel"));
            user.setRegistrationTime(rs.getTimestamp("registration_Time"));

            return user;
        }

    }

    public User read(String userName) throws Exception {
        String sql = "select * from user where accesskey = ?";

        List<User> users = jdbcTemplate.query(sql, new UserRowMapper(), userName);

        return CollectionUtils.isNotEmpty(users) ? users.get(0) : null;
    }

}
