package com.weib.dip.data.platform.services.client;

import com.weib.dip.data.platform.services.client.model.ConfigEntity;

import java.util.List;

/**
 * @author delia
 */

public interface ConfigService {

    ConfigEntity newConfigEntityInstance();

    boolean exist(String user, String key) throws Exception;

    boolean exist(String key) throws Exception;

    int insert(ConfigEntity configEntity) throws Exception;

    int delete(String user, String key) throws Exception;

    int delete(String key) throws Exception;

    String getWithUser(String user, String key) throws Exception;

    String getWithUser(String user, String key, String defaultValue) throws Exception;

    String get(String key) throws Exception;

    String get(String key, String defaultValue) throws Exception;

    ConfigEntity getConfigEntity(String user, String key) throws Exception;

    ConfigEntity getConfigEntity(String key) throws Exception;

    List<ConfigEntity> getUserConfigEntities(String user) throws Exception;

    List<ConfigEntity> getKeyConfigEntities(String key) throws Exception;

    List<ConfigEntity> fuzzyGets(String fuzzyKey) throws Exception;

    String getKeyValue(String filter) throws Exception;

    String getValues(String filter)throws Exception;

    int update(ConfigEntity configEntity) throws Exception;

}
