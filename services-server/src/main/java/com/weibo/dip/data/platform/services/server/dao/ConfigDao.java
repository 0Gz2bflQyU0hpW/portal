package com.weibo.dip.data.platform.services.server.dao;

import com.weib.dip.data.platform.services.client.model.ConfigEntity;

import java.sql.SQLException;
import java.util.List;

/**
 * @author delia
 */
public interface ConfigDao {

    boolean exist(String user, String key) throws SQLException;

    int insert(ConfigEntity configEntity) throws SQLException;

    int delete(String user, String key) throws SQLException;

    String get(String user, String key, String defaultValue) throws SQLException;

    ConfigEntity getConfigEntity(String user, String key) throws SQLException;

    List<ConfigEntity> getUserConfigEntities(String user) throws SQLException;

    List<ConfigEntity> getKeyConfigEntities(String key) throws SQLException;

    List<ConfigEntity> fuzzyGets(String fuzzyKey) throws SQLException;

    int update(ConfigEntity configEntity) throws SQLException;

}
