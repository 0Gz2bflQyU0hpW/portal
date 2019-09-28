package com.weibo.dip.data.platform.datacubic.druid;

import com.weibo.dip.data.platform.datacubic.druid.aggregation.LongSumAggregator;
import com.weibo.dip.data.platform.datacubic.druid.datasource.DataSource;
import com.weibo.dip.data.platform.datacubic.druid.datasource.TableDatasource;
import com.weibo.dip.data.platform.datacubic.druid.dimension.DefaultDimension;
import com.weibo.dip.data.platform.datacubic.druid.query.Granularity;
import com.weibo.dip.data.platform.datacubic.druid.query.GroupBy;
import com.weibo.dip.data.platform.datacubic.druid.query.Interval;
import com.weibo.dip.data.platform.datacubic.druid.query.Row;
import com.weibo.dip.data.platform.datacubic.druid.query.builder.QueryBuilderFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Created by yurun on 17/2/14.
 */
public class DruidGroupByMain {

    public static void main(String[] args) throws Exception {
        DruidClient client = new DruidClient("77-109-197-bx-core.jpool.sinaimg.cn", 18082, 3000, 600000);

        DataSource dataSource = new TableDatasource("fulllink_druid");

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date startTime = format.parse("2017-03-27 00:00:00");

        Date endTime = format.parse("2017-03-28 23:59:59");

        Interval interval = new Interval(startTime, endTime);

        GroupBy groupBy = QueryBuilderFactory.createGroupByBuilder()
            .setDataSource(dataSource)
            .addInterval(interval)
            .setGranularity(Granularity.day)
            .addDimension(new DefaultDimension("app_version"))
            .addDimension(new DefaultDimension("system_version"))
            .addDimension(new DefaultDimension("network_type"))
            .addDimension(new DefaultDimension("city"))
            .addDimension(new DefaultDimension("isp"))
            .addAggregator(new LongSumAggregator("total_num"))
            .addAggregator(new LongSumAggregator("succeed_num"))
            .addAggregator(new LongSumAggregator("neterr_num"))
            .addAggregator(new LongSumAggregator("localerr_num"))
            .addAggregator(new LongSumAggregator("during_time"))
            .addAggregator(new LongSumAggregator("net_time"))
            .addAggregator(new LongSumAggregator("local_time"))
            .build();

        long begin = System.currentTimeMillis();

        List<Row> rows = client.groupBy(groupBy);

        long end = System.currentTimeMillis();

        for (Row row : rows) {
            //System.out.println(row.getRawData());
        }

        System.out.println("rows: " + rows.size() + ", consume: " + (end - begin));
    }

}
