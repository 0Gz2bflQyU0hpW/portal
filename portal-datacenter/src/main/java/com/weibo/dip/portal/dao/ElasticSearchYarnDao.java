package com.weibo.dip.portal.dao;

import org.elasticsearch.action.search.SearchResponse;

/**
 * 
 * @author jianhong
 *
 */

public interface ElasticSearchYarnDao {
	/**
	 * 普通查询
	 * @param clusterId 集群ID
	 * @param applicationName 应用名称
	 * @return
	 */
	public SearchResponse searchScroll(String clusterId, String applicationName);
	/**
	 * 聚合功能的查询
	 * @param clusterId
	 * @param applicationName
	 * @param timeSpan 时间粒度
	 * @return
	 */
	public SearchResponse searchScroll(String clusterId, String applicationName, String timeSpan);
	/**
	 * 
	 * @param scrollId
	 * @return
	 */
	public SearchResponse searchScrollResp(String scrollId);
}
