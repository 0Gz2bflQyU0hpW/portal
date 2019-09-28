package com.weibo.dip.data.platform.commons.util;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by yurun on 16/12/21.
 */
public class ShellUtilMain {

    public static final String SPACE = " ";

    public static final String COLON = ":";

    public static final String PERCENTSIGN = "%";

    public static final String QUOTATION_MARK = "\"";

    public static final String CONFIG_CACHE_SIZE = "config.cache.size";

    public static final String CONFIG_CACHE_DURATION = "config.cache.duration";

    public static final String PYTHON_PATH = "python.path";

    public static final String ALARM_WATCH_PYTHON = "alarm.watch.python";

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

    public static String surroundWithQuotationMark(String str) {
        return QUOTATION_MARK + str + QUOTATION_MARK;
    }

    public static void sendAlarm(String type, String service, String subService, String subject, String content, String[] receivers, boolean weibo, boolean mail, boolean msg) throws Exception {
        Preconditions.checkState(StringUtils.isNotEmpty(type) && (type.equals(USER) || type.equals(GROUP)), "type must be user or group");

        List<String> params = new ArrayList<>();

        params.add("/usr/bin/python");
        params.add("/data0/workspace/python/falcon/util/watchalert.py");

        params.add(TYPE);
        params.add(type);

        params.add(SERVICE);
        params.add(surroundWithQuotationMark(service));

        params.add(SUBSERVICE);
        params.add(surroundWithQuotationMark(subService));

        params.add(SUBJECT);
        params.add(surroundWithQuotationMark(subject));

        params.add(CONTENT);
        params.add(surroundWithQuotationMark(content));

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

        ShellUtil.Result record = ShellUtil.execute(String.join(SPACE, params));

        System.out.println(record);

        if (!record.isSuccess()) {
            throw new RuntimeException(record.getError());
        }
    }

    public static void main(String[] args) throws Exception {
        sendAlarm(USER, "SinaWatch报警系统2", "测试", "用户报警标题", "用户报警内容", new String[]{"yurun"}, true, true, false);
    }

}
