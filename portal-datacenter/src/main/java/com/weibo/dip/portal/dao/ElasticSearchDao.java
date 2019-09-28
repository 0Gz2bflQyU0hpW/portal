package com.weibo.dip.portal.dao;

import com.weibo.dip.portal.model.RawTopic;
import org.elasticsearch.action.search.SearchResponse;

import java.util.List;

public interface ElasticSearchDao {
	public  SearchResponse  searchTopicConsumerAccTime(String indexName, String typeName, String matchType, String topicName, int timeID);
	public SearchResponse searchScrollResp(String scrollId);
	//返回某段时间  聚合的 所有 matchType list
	public List<RawTopic> searchTopicNameListDay(String indexName, String typeName, String matchType, int timeID);
}
