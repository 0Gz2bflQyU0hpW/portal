package com.weibo.dip.data.platform.services.client;

import com.weib.dip.data.platform.services.client.ElasticSearchService;
import com.weib.dip.data.platform.services.client.model.IndexEntity;
import com.weib.dip.data.platform.services.client.util.ServiceProxyBuilder;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by yurun on 16/12/22.
 */
public class ElasticSearchClientApplication {

    public static void main(String[] args) throws Exception {
        ElasticSearchService elasticSearchService = ServiceProxyBuilder.buildLocalhost(ElasticSearchService.class);

        IndexEntity entity = new IndexEntity();

        entity.setIndex("yurun");
        entity.setType("test");
        entity.setId("1");

        entity.setTerm("key1", "value1");
        entity.setTerm("key2", "value2");

        entity.setTimestamp(new Date());

        elasticSearchService.index(entity);

        IndexEntity entity2 = new IndexEntity();

        entity2.setIndex("yurun");
        entity2.setType("test");
        entity2.setId("2");

        entity2.setTerm("key12", "value12");
        entity2.setTerm("key22", "value22");

        entity2.setTimestamp(new Date());

        IndexEntity entity3 = new IndexEntity();

        entity3.setIndex("yurun");
        entity3.setType("test");
        entity3.setId("3");

        entity3.setTerm("key13", "value13");
        entity3.setTerm("key23", "value23");

        entity3.setTimestamp(new Date());

        List<IndexEntity> entities = new ArrayList<>();

        entities.add(entity2);
        entities.add(entity3);

        elasticSearchService.bulkIndex(entities);
    }

}
