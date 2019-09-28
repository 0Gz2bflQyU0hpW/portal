package com.weibo.dip.data.platform.services.server.impl;

import com.google.common.base.Preconditions;
import com.weib.dip.data.platform.services.client.ConfigService;
import com.weib.dip.data.platform.services.client.model.ConfigEntity;
import com.weibo.dip.data.platform.services.server.dao.ConfigDao;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;

import static com.weib.dip.data.platform.services.client.model.ConfigEntity.DEFAULT_USER;

/**
 * @author delia
 */
@Service
public class ConfigServiceImpl implements ConfigService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigServiceImpl.class);

    @Autowired
    private ConfigDao configDao;

    private void validate(String user, String key, String value) throws IllegalStateException {
        Preconditions.checkState(StringUtils.isNotEmpty(user), "Config user must be not empty");
        Preconditions.checkState(StringUtils.isNotEmpty(key), "Config key must be not empty");
        Preconditions.checkState(value != null, "Config value must be not null");
    }

    @Override
    public ConfigEntity newConfigEntityInstance() {
        return new ConfigEntity();
    }

    @Override
    public boolean exist(String user, String key) throws SQLException {
        return configDao.exist(user, key);
    }

    @Override
    public boolean exist(String key) throws SQLException {
        return exist(DEFAULT_USER, key);
    }

    @Override
    public int insert(ConfigEntity configEntity) throws Exception {
        validate(configEntity.getUser(), configEntity.getKey(), configEntity.getValue());

        Preconditions.checkState(!exist(configEntity.getUser(), configEntity.getKey()),
            "Config (" + configEntity.getUser() + ", " + configEntity.getKey() + ") already existed");

        int code = configDao.insert(configEntity);

        if (code > 0) {
            LOGGER.info("Config inserted success: " + configEntity);
        }

        return code;
    }

    @Override
    public int delete(String user, String key) throws Exception {
        Preconditions.checkState(exist(user, key), "Config (" + user + ", " + key + ") does not exist");

        int code = configDao.delete(user, key);

        if (code > 0) {
            LOGGER.info("Config deleted success: (" + user + ", " + key + ")");
        }

        return code;
    }

    @Override
    public int delete(String key) throws Exception {
        return delete(DEFAULT_USER, key);
    }

    @Override
    public String getWithUser(String user, String key) throws Exception {
        String value = configDao.get(user, key, null);

        Preconditions.checkState(value != null, "Config (" + user + ", " + key + ") does not exist");

        return value;
    }

    @Override
    public String getWithUser(String user, String key, String defaultValue) throws Exception {
        return configDao.get(user, key, defaultValue);
    }

    @Override
    public String get(String key) throws Exception {
        return getWithUser(DEFAULT_USER, key);
    }

    @Override
    public String get(String key, String defaultValue) throws Exception {
        return getWithUser(DEFAULT_USER, key, defaultValue);
    }

    @Override
    public ConfigEntity getConfigEntity(String user, String key) throws SQLException {
        return configDao.getConfigEntity(user, key);
    }

    @Override
    public ConfigEntity getConfigEntity(String key) throws SQLException {
        return getConfigEntity(DEFAULT_USER, key);
    }

    @Override
    public List<ConfigEntity> getUserConfigEntities(String user) throws SQLException, IllegalStateException {
        return configDao.getUserConfigEntities(user);
    }

    @Override
    public List<ConfigEntity> getKeyConfigEntities(String key) throws Exception {
        return configDao.getKeyConfigEntities(key);
    }

    @Override
    public List<ConfigEntity> fuzzyGets(String fuzzyKey) throws SQLException, IllegalStateException {
        return configDao.fuzzyGets(fuzzyKey);
    }

    @Override
    public String getKeyValue(String filter) throws Exception {
        List<ConfigEntity> configEntities = fuzzyGets(filter);
        StringBuffer entity=new StringBuffer();
        entity.append("[");
        for (ConfigEntity configEntity : configEntities) {
            entity.append(configEntity.getKey());
            entity.append(",");
            entity.append(configEntity.getValue());
            entity.append(",");
        }
        entity.deleteCharAt(entity.length()-1);
        entity.append("]");
        return entity.toString();
    }

    @Override
    public String getValues(String filter) throws Exception {
        List<ConfigEntity> configEntities = fuzzyGets(filter);
        StringBuffer entity=new StringBuffer();
        entity.append("[");
        for (ConfigEntity configEntity : configEntities) {
            entity.append(configEntity.getValue());
            entity.append(",");
        }
        entity.deleteCharAt(entity.length()-1);
        entity.append("]");
        return entity.toString();
    }
    @Override
    public int update(ConfigEntity configEntity) throws SQLException, IllegalStateException {
        validate(configEntity.getUser(), configEntity.getKey(), configEntity.getValue());

        Preconditions.checkState(exist(configEntity.getUser(), configEntity.getKey()),
            "Config (" + configEntity.getUser() + ", " + configEntity.getKey() + ") dose not exist");

        int code = configDao.update(configEntity);

        if (code > 0) {
            LOGGER.info("Config updated success: " + configEntity);
        }

        return code;
    }

}
