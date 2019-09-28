package com.weibo.dip.data.platform.datacubic.druid;

import com.weibo.dip.data.platform.datacubic.druid.filter.Filter;
import com.weibo.dip.data.platform.datacubic.druid.filter.SelectorFilter;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * Created by yurun on 17/2/14.
 */
public class DruidDistinctMain2 {

    public static void main(String[] args) throws Exception {
        DruidClient client = new DruidClient("77-109-197-bx-core.jpool.sinaimg.cn", 18082, 3000, 600000);

        String dataSource = "fulllink_druid";

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date startTime = format.parse("2017-03-27 00:00:00");

        Date endTime = format.parse("2017-03-28 23:59:59");

        Filter filter = new SelectorFilter("app_version", "7.3.0");

        String dimension = "system_version";

        String[] dimensions = client.distinct(dataSource, startTime, endTime, filter, dimension);

        System.out.println("dimensions: " + Arrays.toString(dimensions));
    }

}
