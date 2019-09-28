package com.weib.dip.data.platform.services.client;

import com.weib.dip.data.platform.services.client.model.IndexEntity;

import java.util.List;

/**
 * Created by yurun on 16/12/22.
 */
public interface ElasticSearchService {

    IndexEntity newIndexEntityInstance();

    void index(IndexEntity entity) throws Exception;

    void bulkIndex(List<IndexEntity> entities) throws Exception;

}
