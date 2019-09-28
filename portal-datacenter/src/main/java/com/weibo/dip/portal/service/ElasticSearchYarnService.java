package com.weibo.dip.portal.service;

import com.weibo.dip.portal.model.Container;

import java.util.List;
import java.util.Map;

public interface ElasticSearchYarnService {


	
	/**
	 * 查询某个Application对应的所有Container
	 * @param clusterId
	 * @param applicationName
	 * @return Map集合,key:containerName，value：与containerName相关所有container信息
	 */
	public Map<String, List<Container>> searchContainer(String clusterId, String applicationName);

	/**
	 * 具有聚合功能的查询某个Application对应的所有Container
	 * @param clusterId
	 * @param applicationName
	 * @param timeSpan 时间粒度
	 * @return
	 */
	public Map<String, List<Container>> searchContainer(String clusterId, String applicationName, String timeSpan);
}

