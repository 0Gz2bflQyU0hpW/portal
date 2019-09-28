package com.weibo.dip.portal.service;

import com.weibo.dip.portal.model.RawTopic;
import com.weibo.dip.portal.model.Topic;

import java.util.List;

public interface ElasticSearchService {
	public List<Topic> searchTopicConsumerAccTime(String indexName, String typeName, String matchType, String topicName, int timeID);
	public List<RawTopic> searchTopicNameListDay(String indexName, String typeName, String matchType, int timeID);
}
