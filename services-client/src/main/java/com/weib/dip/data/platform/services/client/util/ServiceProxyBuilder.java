package com.weib.dip.data.platform.services.client.util;

import com.caucho.hessian.client.HessianProxyFactory;

/**
 * Created by yurun on 16/12/20.
 */
public class ServiceProxyBuilder {

    private static final String DEFAULT_HOST = "services.data.platform.intra.dip.weibo.com";

    private static final String DEFAULT_LOCALHOST = "localhost";

    private static final int DEFAULT_PORT = 8443;

    private static final String SERVICES = "services";

    private static final String DEFAULT_USERNAME = "dip";

    private static final String DEFAULT_PASSWORD = "dipadmin";

    private static final boolean DEFAULT_OVERLOAD = true;

    private static final long DEFAULT_CONNECTTIMEOUT = 5000;

    private static final long DEFAULT_READTIMEOUT = 30000;

    @SuppressWarnings("unchecked")
    public static <T> T build(String host, int port, Class<T> iface, String serviceUrl, String username, String password, boolean overload, long connectTimeout, long readTimeout) {
        String url = "http://" + host + ":" + port + serviceUrl;

        HessianProxyFactory factory = new HessianProxyFactory();

        factory.setUser(username);
        factory.setPassword(password);

        factory.setOverloadEnabled(overload);

        factory.setConnectTimeout(connectTimeout);
        factory.setReadTimeout(readTimeout);

        try {
            return (T) factory.create(iface, url);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String getServiceUrl(Class<?> iface) {
        String ifaceName = iface.getSimpleName();

        return "/" + SERVICES + "/" + ifaceName.substring(0, 1).toLowerCase() + ifaceName.substring(1);
    }

    public static <T> T buildLocalhost(Class<T> iface) {
        return build(DEFAULT_LOCALHOST, DEFAULT_PORT, iface, getServiceUrl(iface), DEFAULT_USERNAME, DEFAULT_PASSWORD, DEFAULT_OVERLOAD, DEFAULT_CONNECTTIMEOUT, DEFAULT_READTIMEOUT);
    }

    public static <T> T build(Class<T> iface) {
        return build(DEFAULT_HOST, DEFAULT_PORT, iface, getServiceUrl(iface), DEFAULT_USERNAME, DEFAULT_PASSWORD, DEFAULT_OVERLOAD, DEFAULT_CONNECTTIMEOUT, DEFAULT_READTIMEOUT);
    }

}
