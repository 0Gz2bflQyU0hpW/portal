package com.weibo.dip.portal.service.impl;

import com.weibo.dip.portal.dao.ElasticSearchDao;
import com.weibo.dip.portal.model.Consumer;
import com.weibo.dip.portal.model.RawTopic;
import com.weibo.dip.portal.model.Topic;
import com.weibo.dip.portal.service.ElasticSearchService;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Service("elasticSearchService")
@Transactional
public class ElasticSearchServiceImpl implements ElasticSearchService {

	@Autowired
	private ElasticSearchDao elasticSearchDao;

	/**
	 * 
	 * @param indexName
	 * @param typeName
	 * @param matchType
	 *            得到特定topicName的信息。这里德尔matchType="topic_name"
	 * @param matchName
	 *            topic的名字
	 * @param limit
	 * @return
	 */
	public List<Topic> searchTopicConsumerAccTime(String indexName, String typeName, String matchType, String topicName,
			int timeID) {

		// Scroll until no hits are returned
		SearchResponse scrollResp = elasticSearchDao.searchTopicConsumerAccTime(indexName, typeName, matchType,
				topicName, timeID);
		List<Topic> topicList = new ArrayList<Topic>();
		String calTime;
		String logSize = "";

		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				Topic topic = new Topic();
				calTime = hit.getSource().get("cal_time").toString();
				logSize = hit.getSource().get("log_size").toString();
				@SuppressWarnings("unchecked")
				List<Consumer> consumerList = (List<Consumer>) hit.getSource().get("consumer_info_list");
				topic.setCalTime(calTime);
				topic.setLogSize(logSize);
				topic.setConsumerList(consumerList);
				topic.setTopicName(topicName);

				topicList.add(topic);
			}
			scrollResp = elasticSearchDao.searchScrollResp(scrollResp.getScrollId());
			// Break condition: No hits are returned
			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}
		}
		return topicList;
	}

	@Override
	public List<RawTopic> searchTopicNameListDay(String indexName,
			String typeName, String matchType, int timeID) {
		List<RawTopic> topicList = elasticSearchDao.searchTopicNameListDay(indexName, typeName, matchType, timeID);
		return topicList;
	}

}
