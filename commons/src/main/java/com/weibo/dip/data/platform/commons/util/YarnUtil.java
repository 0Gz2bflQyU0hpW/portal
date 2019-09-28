package com.weibo.dip.data.platform.commons.util;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/**
 * Created by yurun on 17/11/3.
 */
public class YarnUtil {

    private static final YarnClient YARN_CLIENT = YarnClient.createYarnClient();

    static {
        YARN_CLIENT.init(HadoopConfiguration.getInstance());

        YARN_CLIENT.start();
    }

    public static List<ApplicationReport> getApplications(EnumSet<YarnApplicationState> states)
        throws IOException, YarnException {
        return YARN_CLIENT.getApplications(states);
    }

    public static List<ApplicationReport> getApplications() throws IOException, YarnException {
        return getApplications(EnumSet.allOf(YarnApplicationState.class));
    }

    public static List<ApplicationReport> getApplications(YarnApplicationState state)
        throws IOException, YarnException {
        if (Objects.isNull(state)) {
            return null;
        }

        return getApplications(EnumSet.of(state));
    }

    public static ApplicationReport getApplication(String appName, EnumSet<YarnApplicationState> states)
        throws IOException, YarnException {
        if (StringUtils.isEmpty(appName) || Objects.isNull(states)) {
            return null;
        }

        List<ApplicationReport> applicationReports = getApplications(states);

        if (CollectionUtils.isNotEmpty(applicationReports)) {
            for (ApplicationReport applicationReport : applicationReports) {
                if (applicationReport.getName().equals(appName)) {
                    return applicationReport;
                }
            }
        }

        return null;
    }

    public static ApplicationReport getApplication(String appName) throws IOException, YarnException {
        return getApplication(appName, EnumSet.allOf(YarnApplicationState.class));
    }

    public static ApplicationReport getApplication(String appName, YarnApplicationState state)
        throws IOException, YarnException {
        return getApplication(appName, EnumSet.of(state));
    }

}
