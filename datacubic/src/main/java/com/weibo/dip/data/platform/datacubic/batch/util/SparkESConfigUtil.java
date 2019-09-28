package com.weibo.dip.data.platform.datacubic.batch.util;

import com.google.common.base.Preconditions;
import org.apache.commons.codec.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Created by xiaoyu on 2017/3/14.
 */
public class SparkESConfigUtil {

    private static Logger LOGGER = LoggerFactory.getLogger(SparkESConfigUtil.class);
    public static String ES_CONF = "es.config";
    public static String ES_NODES_KEY = "es.nodes";
    public static String ES_PORT_KEY = "es.port";
    public static String ES_NET_HTTP_AUTH_USER_KEY = "es.net.http.auth.user";
    public static String ES_NET_HTTP_AUTH_PASS_KEY = "es.net.http.auth.pass";
    public static String ES_INDEX_AND_TYPE_KEY = "es.index.and.type";

    public static String ES_NODES;
    public static String ES_PORT;
    public static String ES_NET_HTTP_AUTH_USER;
    public static String ES_NET_HTTP_AUTH_PASS;
    public static String ES_INDEX_AND_TYPE;

    static {
        Properties properties = new Properties();

        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new InputStreamReader(SparkESConfigUtil.class.getClassLoader().getResourceAsStream(ES_CONF), CharEncoding.UTF_8));
            properties.load(reader);
            ES_NODES = properties.getProperty(ES_NODES_KEY);
            Preconditions.checkState(StringUtils.isNotEmpty(ES_NODES), ES_NODES_KEY + " must be specified.");

            ES_PORT = properties.getProperty(ES_PORT_KEY);
            Preconditions.checkState(StringUtils.isNotEmpty(ES_PORT), ES_PORT_KEY + " must be specified.");

            ES_NET_HTTP_AUTH_USER = properties.getProperty(ES_NET_HTTP_AUTH_USER_KEY);
            Preconditions.checkState(StringUtils.isNotEmpty(ES_NET_HTTP_AUTH_USER), ES_NET_HTTP_AUTH_USER_KEY + " must be specified.");

            ES_NET_HTTP_AUTH_PASS = properties.getProperty(ES_NET_HTTP_AUTH_PASS_KEY);
            Preconditions.checkState(StringUtils.isNotEmpty(ES_NET_HTTP_AUTH_PASS), ES_NET_HTTP_AUTH_PASS_KEY + " must be specified.");

            ES_INDEX_AND_TYPE = properties.getProperty(ES_INDEX_AND_TYPE_KEY);
            Preconditions.checkState(StringUtils.isNotEmpty(ES_INDEX_AND_TYPE), ES_INDEX_AND_TYPE_KEY + " must be specified.");

            LOGGER.info("[ " + ES_NODES_KEY + ":" + ES_NODES + " " + ES_PORT_KEY + ":" + ES_PORT + " " + ES_NET_HTTP_AUTH_USER_KEY + ":" + ES_NET_HTTP_AUTH_USER + " " + ES_NET_HTTP_AUTH_PASS_KEY + ":" + ES_NET_HTTP_AUTH_PASS + " " + ES_INDEX_AND_TYPE_KEY + ":" + ES_INDEX_AND_TYPE + "]");
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    LOGGER.error(ES_CONF + " reader close error: " + ExceptionUtils.getFullStackTrace(e));
                }
            }
        }
    }
}
