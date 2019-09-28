package com.weibo.dip.data.platform.services.server.impl;

import com.google.common.base.Preconditions;
import com.weib.dip.data.platform.services.client.AlarmService;
import com.weibo.dip.data.platform.commons.util.ShellUtil;
import com.weibo.dip.data.platform.services.server.util.Conf;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by yurun on 16/12/21.
 */
@Service
public class AlarmServiceImpl implements AlarmService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlarmServiceImpl.class);

    private static final String USER = "user";

    private static final String GROUP = "group";

    private static final String TYPE = "-type";

    private static final String SERVICE = "-service";

    private static final String SUBSERVICE = "-subservice";

    private static final String SUBJECT = "-subject";

    private static final String CONTENT = "-content";

    private static final String RECEIVERS = "-receivers";

    private static final String WEIBO = "-weibo";

    private static final String MAIL = "-mail";

    private static final String MSG = "-msg";

    @Autowired
    private Environment env;

    private String encode(String str) throws UnsupportedEncodingException {
        return URLEncoder.encode(str, CharEncoding.UTF_8);
    }

    private void sendAlarm(String type, String service, String subService, String subject, String content, String[] receivers, boolean weibo, boolean mail, boolean msg) throws Exception {
        LOGGER.warn(String.format("Alarm: %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s, %s %s", TYPE, type, SERVICE, service, SUBSERVICE, subService, SUBJECT, subject, CONTENT, content, RECEIVERS, String.join(",", (CharSequence[]) receivers), WEIBO, weibo, MAIL, mail, MSG, msg));

        Preconditions.checkState(StringUtils.isNotEmpty(type) && (type.equals(USER) || type.equals(GROUP)), "type must be user or group");

        List<String> params = new ArrayList<>();

        params.add(env.getProperty(Conf.PYTHON_PATH) + Conf.SPACE + env.getProperty(Conf.ALARM_WATCH_PYTHON));

        params.add(TYPE);
        params.add(type);

        params.add(SERVICE);
        params.add(encode(service));

        params.add(SUBSERVICE);
        params.add(encode(subService));

        params.add(SUBJECT);
        params.add(encode(subject));

        params.add(CONTENT);
        params.add(encode(content));

        params.add(RECEIVERS);
        params.add(String.join(",", Arrays.asList(receivers)));

        if (weibo) {
            params.add(WEIBO);
        }

        if (mail) {
            params.add(MAIL);
        }

        if (msg) {
            params.add(MSG);
        }

        ShellUtil.Result record = ShellUtil.execute(String.join(Conf.SPACE, params));

        if (!record.isSuccess()) {
            throw new RuntimeException("excute " + env.getProperty(Conf.ALARM_WATCH_PYTHON) + " error: " + record.getError());
        }
    }

    @Override
    public void sendAlarmToUsers(String service, String subService, String subject, String content, String[] users) {
        try {
            sendAlarm(USER, service, subService, subject, content, users, true, true, false);
        } catch (Exception e) {
            LOGGER.error("sendAlarmToUsers error: " + ExceptionUtils.getFullStackTrace(e));
        }
    }

    @Override
    public void sendAlarmToGroups(String service, String subService, String subject, String content, String[] groups) {
        try {
            sendAlarm(GROUP, service, subService, subject, content, groups, true, true, false);
        } catch (Exception e) {
            LOGGER.error("sendAlarmToGroups error: " + ExceptionUtils.getFullStackTrace(e));
        }
    }

}
