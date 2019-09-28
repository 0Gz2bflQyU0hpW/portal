package com.weibo.dip.data.platform.datacubic.druid;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.druid.aggregation.CountAggregator;
import com.weibo.dip.data.platform.datacubic.druid.datasource.TableDatasource;
import com.weibo.dip.data.platform.datacubic.druid.dimension.DefaultDimension;
import com.weibo.dip.data.platform.datacubic.druid.filter.Filter;
import com.weibo.dip.data.platform.datacubic.druid.query.*;
import com.weibo.dip.data.platform.datacubic.druid.query.builder.QueryBuilderFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by yurun on 17/1/23.
 */
public class DruidClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(DruidClient.class);

    private static final String CONTENT_TYPE = "Content-Type";

    private static final String APPLICATION_JSON = "application/json";

    private String domain;

    private int port;

    private int connectionTimeout;

    private int soTimeout;

    private String url;

    public DruidClient(String domain, int port, int connectionTimeout, int soTimeout) {
        this.domain = domain;
        this.port = port;
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;

        url = ("http://" + domain + ":" + port + "/druid/v2/?pretty");
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    private JsonArray exec(String request) throws Exception {
        HttpClient client = new HttpClient();

        client.getHttpConnectionManager().getParams().setConnectionTimeout(connectionTimeout);
        client.getHttpConnectionManager().getParams().setSoTimeout(soTimeout);

        PostMethod post = new PostMethod(url);

        post.addRequestHeader(CONTENT_TYPE, APPLICATION_JSON);

        LOGGER.info("request: " + request);

        post.setRequestEntity(new StringRequestEntity(request, null, CharEncoding.UTF_8));

        try {
            client.executeMethod(post);

            String result = StringUtils.join(IOUtils.readLines(post.getResponseBodyAsStream(), CharEncoding.UTF_8), "\n");

            LOGGER.debug("exec result: " + result);

            JsonElement response = GsonUtil.parse(result);

            if (!response.isJsonArray()) {
                throw new RuntimeException(result);
            }

            return response.getAsJsonArray();
        } finally {
            post.releaseConnection();
        }
    }

    public List<Row> groupBy(GroupBy groupBy) throws Exception {
        JsonArray elements = exec(groupBy.getQuery());

        List<Row> datas = new ArrayList<>();

        for (JsonElement element : elements) {
            String timestamp = element.getAsJsonObject().getAsJsonPrimitive("timestamp").getAsString();

            JsonObject event = element.getAsJsonObject().getAsJsonObject("event");

            event.addProperty("timestamp", timestamp);

            datas.add(new Row(event.toString()));

        }

        return datas;
    }

    public List<Row> timeseries(Timeseries timeseries) throws Exception {
        JsonArray elements = exec(timeseries.getQuery());

        List<Row> datas = new ArrayList<>();

        for (JsonElement element : elements) {
            String timestamp = element.getAsJsonObject().getAsJsonPrimitive("timestamp").getAsString();

            JsonObject event = element.getAsJsonObject().getAsJsonObject("result");

            event.addProperty("timestamp", timestamp);

            datas.add(new Row(event.toString()));

        }

        return datas;
    }

    public String[] distinct(String dataSource, Date startTime, Date endTime, String dimension) throws Exception {
        return distinct(dataSource, startTime, endTime, null, dimension);
    }

    public String[] distinct(String dataSource, Date startTime, Date endTime, Filter filter, String dimension) throws Exception {
        GroupBy groupBy = QueryBuilderFactory.createGroupByBuilder()
            .setDataSource(new TableDatasource(dataSource))
            .addInterval(new Interval(startTime, endTime))
            .setGranularity(Granularity.all)
            .addDimension(new DefaultDimension(dimension))
            .setFilter(filter)
            .addAggregator(new CountAggregator())
            .build();

        List<Row> rows = groupBy(groupBy);

        if (CollectionUtils.isEmpty(rows)) {
            return null;
        }

        Set<String> dimensions = new HashSet<>();

        for (Row row : rows) {
            dimensions.add(row.getString(dimension));
        }

        return dimensions.toArray(new String[dimensions.size()]);
    }

}
