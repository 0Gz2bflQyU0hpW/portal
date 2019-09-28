package com.weibo.dip.data.platform.datacubic.druid;

import com.weibo.dip.data.platform.datacubic.druid.aggregation.LongSumAggregator;
import com.weibo.dip.data.platform.datacubic.druid.aggregation.post.ArithmeticPostAggregator;
import com.weibo.dip.data.platform.datacubic.druid.aggregation.post.FieldAccessorPostAggregator;
import com.weibo.dip.data.platform.datacubic.druid.datasource.DataSource;
import com.weibo.dip.data.platform.datacubic.druid.datasource.TableDatasource;
import com.weibo.dip.data.platform.datacubic.druid.dimension.DefaultDimension;
import com.weibo.dip.data.platform.datacubic.druid.having.AndHaving;
import com.weibo.dip.data.platform.datacubic.druid.having.DimemsionHaving;
import com.weibo.dip.data.platform.datacubic.druid.having.Having;
import com.weibo.dip.data.platform.datacubic.druid.having.MetricHaving;
import com.weibo.dip.data.platform.datacubic.druid.limit.DefaultLimitSpec;
import com.weibo.dip.data.platform.datacubic.druid.limit.LimitSpec;
import com.weibo.dip.data.platform.datacubic.druid.limit.OrderByColumnSpec;
import com.weibo.dip.data.platform.datacubic.druid.query.*;
import com.weibo.dip.data.platform.datacubic.druid.query.builder.QueryBuilderFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Created by yurun on 17/2/14.
 */
public class DruidGroupByMain2 {

    public static void main(String[] args) throws Exception {
        DruidClient client = new DruidClient("77-109-197-bx-core.jpool.sinaimg.cn", 18082, 3000, 600000);

        DataSource dataSource = new TableDatasource("fulllink_druid");

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date startTime = format.parse("2017-03-27 00:00:00");

        Date endTime = format.parse("2017-03-28 23:59:59");

        Interval interval = new Interval(startTime, endTime);

        LimitSpec limitSpec = new DefaultLimitSpec(30, new OrderByColumnSpec("app_version", OrderByColumnSpec.DESCENDING, SortingOrder.lexicographic));

        Having metricHaving = new MetricHaving(MetricHaving.GREATER_THAN, "total_num", 500000);

        Having dimensionHaving = new DimemsionHaving("app_version", "7.3.0");

        Having andHaving = new AndHaving(metricHaving, dimensionHaving);

        GroupBy groupBy = QueryBuilderFactory.createGroupByBuilder()
            .setDataSource(dataSource)
            .addInterval(interval)
            .setGranularity(Granularity.day)
            .addDimension(new DefaultDimension("app_version"))
            .setLimitSpec(limitSpec)
            .setHaving(andHaving)
            .addAggregator(new LongSumAggregator("total_num"))
            .addAggregator(new LongSumAggregator("succeed_num"))
            .addPostAggregator(new ArithmeticPostAggregator("test_plus", ArithmeticPostAggregator.PLUS, new FieldAccessorPostAggregator("total_num"), new FieldAccessorPostAggregator("succeed_num")))
            .addPostAggregator(new ArithmeticPostAggregator("test_minus", ArithmeticPostAggregator.MINUS, new FieldAccessorPostAggregator("test_plus"), new FieldAccessorPostAggregator("succeed_num")))
            .build();

        long begin = System.currentTimeMillis();

        List<Row> rows = client.groupBy(groupBy);

        long end = System.currentTimeMillis();

        for (Row row : rows) {
            System.out.println(row.getRawData());
        }

        System.out.println("rows: " + rows.size() + ", consume: " + (end - begin));
    }

}
