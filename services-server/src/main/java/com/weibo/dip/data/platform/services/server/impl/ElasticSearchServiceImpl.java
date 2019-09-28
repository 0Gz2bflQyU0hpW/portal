package com.weibo.dip.data.platform.services.server.impl;

import com.weib.dip.data.platform.services.client.ElasticSearchService;
import com.weib.dip.data.platform.services.client.model.IndexEntity;
import com.weibo.dip.data.platform.services.server.util.Conf;
import com.weibo.dip.data.platform.services.server.util.TransportClientBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Created by yurun on 16/12/22.
 */
@Service
public class ElasticSearchServiceImpl implements ElasticSearchService {

    private TransportClient client;
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchServiceImpl.class);

    @Autowired
    public ElasticSearchServiceImpl(Environment env) {
        try {
            client = TransportClientBuilder.build(env.getProperty(Conf.ES_CLUSTER_NAME), env.getProperty(Conf.ES_STORE_PATH),
                    env.getProperty(Conf.ES_KEYSTORE), env.getProperty(Conf.ES_TRUSTSTORE), env.getProperty(Conf.ES_KEYSTORE_PASSWORD),
                    env.getProperty(Conf.ES_TRUSTSTORE_PASSWORD), env.getProperty(Conf.ES_DOMAIN), Integer.valueOf(env.getProperty(Conf.ES_PORT)));
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public IndexEntity newIndexEntityInstance() {
        return new IndexEntity();
    }

    private IndexRequestBuilder prepareIndex(IndexEntity entity) throws Exception {
        entity.checkState();

        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();

        for (Map.Entry<String, Object> entry : entity.getTerms().entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }

        builder.field("@timestamp", entity.getTimestamp());

        builder.endObject();

        IndexRequestBuilder index;

        if (StringUtils.isEmpty(entity.getId())) {
            index = client.prepareIndex(entity.getIndex(), entity.getType());
        } else {
            index = client.prepareIndex(entity.getIndex(), entity.getType(), entity.getId());
        }

        return index.setSource(builder);
    }

    @Override
    public void index(IndexEntity entity) throws Exception {
        if (Objects.isNull(entity)) {
            return;
        }

        prepareIndex(entity).get();
    }

    @Override
    public void bulkIndex(List<IndexEntity> entities) throws Exception {
        if (CollectionUtils.isEmpty(entities)) {
            return;
        }

        BulkRequestBuilder bulk = this.client.prepareBulk();
        for (IndexEntity entity : entities) {
             bulk.add(prepareIndex(entity));
        }

         bulk.get();
    }


}
