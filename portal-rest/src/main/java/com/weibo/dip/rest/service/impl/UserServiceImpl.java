package com.weibo.dip.rest.service.impl;

import com.weibo.dip.rest.bean.User;
import com.weibo.dip.rest.dao.UserDao;
import com.weibo.dip.rest.service.UserService;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by yurun on 17/10/20.
 */
@Service
public class UserServiceImpl implements UserService {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserServiceImpl.class);

    @Autowired
    private UserDao userDao;

    public User read(String accesskey) {
        User user;

        try {
            user = userDao.read(accesskey);
        } catch (Exception e) {
            user = null;
            
            LOGGER.error("read user error: {}", ExceptionUtils.getFullStackTrace(e));
        }

        return user;
    }

}
