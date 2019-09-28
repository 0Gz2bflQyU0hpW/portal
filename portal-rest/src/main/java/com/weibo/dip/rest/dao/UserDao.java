package com.weibo.dip.rest.dao;

import com.weibo.dip.rest.bean.User;

/**
 * Created by yurun on 17/10/25.
 */
public interface UserDao {

    User read(String accesskey) throws Exception;

}
