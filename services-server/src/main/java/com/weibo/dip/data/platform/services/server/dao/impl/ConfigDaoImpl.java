package com.weibo.dip.data.platform.services.server.dao.impl;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.weib.dip.data.platform.services.client.model.ConfigEntity;
import com.weibo.dip.data.platform.services.server.dao.ConfigDao;
import com.weibo.dip.data.platform.services.server.util.Conf;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author delia
 */

@Repository
public class ConfigDaoImpl implements ConfigDao {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private Cache<String, String> cache = null;

    @Autowired
    public ConfigDaoImpl(Environment env) {
        try {
            long size = Long.parseLong(env.getProperty(Conf.CONFIG_CACHE_SIZE));
            long duration = Long.parseLong(env.getProperty(Conf.CONFIG_CACHE_DURATION));

            cache = CacheBuilder.newBuilder()
                .maximumSize(size)
                .expireAfterWrite(duration, TimeUnit.SECONDS)
                .recordStats()
                .build();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public boolean exist(String user, String key) throws SQLException {
        String sql = "select count(1) from config_table where conf_user = ? and conf_key = ?";

        return jdbcTemplate.queryForObject(sql, Integer.class, user, key) > 0;
    }

    @Override
    public int insert(ConfigEntity configEntity) throws SQLException {
        String sql = "insert into config_table(conf_user, conf_key, conf_value, conf_iscached, conf_comment) values(?, ?, ?, ?, ?)";

        return jdbcTemplate.update(sql, configEntity.getUser(), configEntity.getKey(), configEntity.getValue(),
            configEntity.isCached(), configEntity.getComment());
    }

    @Override
    public int delete(String user, String key) throws SQLException {
        String sql = "delete from config_table where conf_user = ? and conf_key = ?";

        return jdbcTemplate.update(sql, user, key);
    }

    @Override
    public String get(String user, String key, String defaultValue) throws SQLException {
        String cacheKey = user + Conf.COLON + key;

        String value = cache.getIfPresent(cacheKey);
        if (value != null) {
            return value;
        }

        ConfigEntity configEntity = getConfigEntity(user, key);
        if (configEntity == null) {
            return defaultValue;
        }

        value = configEntity.getValue();
        if (configEntity.isCached()) {
            cache.put(cacheKey, value);
        }

        return value;
    }

    private static class ConfigEntityRowMapper implements RowMapper<ConfigEntity> {

        private static final String CONF_USER = "conf_user";
        private static final String CONF_KEY = "conf_key";
        private static final String CONF_VALUE = "conf_value";
        private static final String CONF_ISCACHE = "conf_iscached";
        private static final String CONF_COMMENT = "conf_comment";
        private static final String CONF_UPDATETIME = "conf_updatetime";

        @Override
        public ConfigEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
            ConfigEntity configEntity = new ConfigEntity();

            configEntity.setUser(rs.getString(CONF_USER));
            configEntity.setKey(rs.getString(CONF_KEY));
            configEntity.setValue(rs.getString(CONF_VALUE));
            configEntity.setCached(rs.getBoolean(CONF_ISCACHE));
            configEntity.setComment(rs.getString(CONF_COMMENT));
            configEntity.setUpdateTime(new Date(rs.getTimestamp(CONF_UPDATETIME).getTime()));

            return configEntity;
        }

    }

    @Override
    public ConfigEntity getConfigEntity(String user, String key) throws SQLException {
        String sql = "select * from config_table where conf_user = ? and conf_key = ?";

        List<ConfigEntity> configEntities = jdbcTemplate.query(sql, new ConfigEntityRowMapper(), user, key);

        return CollectionUtils.isNotEmpty(configEntities) ? configEntities.get(0) : null;
    }

    @Override
    public List<ConfigEntity> getUserConfigEntities(String user) throws SQLException {
        return jdbcTemplate.query("select * from config_table where conf_user = ?", new ConfigEntityRowMapper(), user);
    }

    @Override
    public List<ConfigEntity> getKeyConfigEntities(String key) throws SQLException {
        return jdbcTemplate.query("select * from config_table where conf_key = ?", new ConfigEntityRowMapper(), key);
    }

    @Override
    public List<ConfigEntity> fuzzyGets(String fuzzyKey) throws SQLException {
        return jdbcTemplate.query("select * from config_table where conf_key like ?", new ConfigEntityRowMapper(), Conf.PERCENTSIGN + fuzzyKey + Conf.PERCENTSIGN);
    }

    @Override
    public int update(ConfigEntity configEntity) throws SQLException {
        String sql = "update config_table set conf_value = ?, conf_iscached = ?, conf_comment = ? where conf_user = ? and conf_key = ?";

        return jdbcTemplate.update(sql, configEntity.getValue(), configEntity.isCached(), configEntity.getComment(),
            configEntity.getUser(), configEntity.getKey());
    }

}
