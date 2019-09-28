package com.weibo.dip.portal.conf;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

/**
 * 
 * @author jianhong
 *
 */
@Repository("YarnMonitorSettings")
public class YarnMonitorSettings {
	@Value("${yarn.cluster.id.list}")
	private String clusterIdList;
	
	@Value("${yarn.cluster.resourcemanager.list}")
	private String resourceManagerList;			
	
	public String getClusterIdList() {
		return clusterIdList;
	}

	public void setClusterIdList(String clusterIdList) {
		this.clusterIdList = clusterIdList;
	}

	public String getResourceManagerList() {
		return resourceManagerList;
	}

	public void setResourceManagerList(String resourceManagerList) {
		this.resourceManagerList = resourceManagerList;
	}

}
