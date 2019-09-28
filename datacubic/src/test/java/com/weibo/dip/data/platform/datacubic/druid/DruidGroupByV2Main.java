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
public class DruidGroupByV2Main {

    public static void main(String[] args) throws Exception {
        DruidClient client = new DruidClient("77-109-197-bx-core.jpool.sinaimg.cn", 18082, 3000, 600000);

        DataSource dataSource = new TableDatasource("fulllink_v2");

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date startTime = format.parse("2017-03-30 00:00:00");

        Date endTime = format.parse("2017-03-30 23:59:59");

        Interval interval = new Interval(startTime, endTime);

        GroupBy groupBy = QueryBuilderFactory.createGroupByBuilder()
            .setDataSource(dataSource)
            .addInterval(interval)
            .setGranularity(Granularity.day)
            .addDimension(new DefaultDimension("act"))
            .addDimension(new DefaultDimension("subtype"))
            .addDimension(new DefaultDimension("app_version"))
            .addDimension(new DefaultDimension("system_version"))
            .addDimension(new DefaultDimension("network_type"))
            .addDimension(new DefaultDimension("city"))
            .addDimension(new DefaultDimension("isp"))
            .addDimension(new DefaultDimension("sch"))
            .addDimension(new DefaultDimension("request_url"))
            .addAggregator(new LongSumAggregator("total_num"))
            .addAggregator(new LongSumAggregator("succeed_num"))
            .addAggregator(new LongSumAggregator("neterr_num"))
            .addAggregator(new LongSumAggregator("localerr_num"))
            .addAggregator(new LongSumAggregator("during_time"))
            .addAggregator(new LongSumAggregator("during_time_1s"))
            .addAggregator(new LongSumAggregator("during_time_2s"))
            .addAggregator(new LongSumAggregator("during_time_3s"))
            .addAggregator(new LongSumAggregator("during_time_4s"))
            .addAggregator(new LongSumAggregator("during_time_5s"))
            .addAggregator(new LongSumAggregator("during_time_long"))
            .addAggregator(new LongSumAggregator("net_time"))
            .addAggregator(new LongSumAggregator("net_time_1s"))
            .addAggregator(new LongSumAggregator("net_time_2s"))
            .addAggregator(new LongSumAggregator("net_time_3s"))
            .addAggregator(new LongSumAggregator("net_time_4s"))
            .addAggregator(new LongSumAggregator("net_time_5s"))
            .addAggregator(new LongSumAggregator("net_time_long"))
            .addAggregator(new LongSumAggregator("local_time"))
            .addAggregator(new LongSumAggregator("parse_time"))
            .addAggregator(new LongSumAggregator("parse_time_1s"))
            .addAggregator(new LongSumAggregator("parse_time_2s"))
            .addAggregator(new LongSumAggregator("parse_time_3s"))
            .addAggregator(new LongSumAggregator("parse_time_4s"))
            .addAggregator(new LongSumAggregator("parse_time_5s"))
            .addAggregator(new LongSumAggregator("parse_time_long"))
            .addAggregator(new LongSumAggregator("lw"))
            .addAggregator(new LongSumAggregator("dl"))
            .addAggregator(new LongSumAggregator("sc"))
            .addAggregator(new LongSumAggregator("ssc"))
            .addAggregator(new LongSumAggregator("sr"))
            .addAggregator(new LongSumAggregator("ws"))
            .addAggregator(new LongSumAggregator("rh"))
            .addAggregator(new LongSumAggregator("rb"))
            .addAggregator(new LongSumAggregator("ne"))
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
