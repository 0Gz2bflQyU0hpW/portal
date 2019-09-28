package com.weibo.dip.portal.service;

import org.apache.commons.httpclient.HttpException;
import org.dom4j.DocumentException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 
 * @author jianhong
 *
 */

public interface ResourceManagerService {
	
	/**
	 * 获取queueName列表
	 * @param clusterId 
	 * @return
	 * @throws IOException 
	 * @throws HttpException 
	 * @throws DocumentException 
	 */
	public List<String> getQueueName(String clusterId) throws HttpException, IOException, DocumentException;
	
	/**
	 * 获取userName列表
	 * @param queueName
	 * @return
	 * @throws IOException 
	 * @throws HttpException 
	 * @throws DocumentException 
	 */
	public Set<String> getUserName(String clusterId, String queueName) throws HttpException, IOException, DocumentException;
	
	/**
	 * 获取applicationType集合
	 * @param queueName
	 * @param userName
	 * @return
	 * @throws DocumentException 
	 * @throws IOException 
	 * @throws HttpException 
	 */
	public Set<String> getApplicationType(String clusterId, String queueName, String userName) throws HttpException, IOException, DocumentException;
	
	/**
	 * 获取applicationName的map，key为applicationName，value为applicationId,startedTime
	 * @param queueName
	 * @param userName
	 * @param applicationType
	 * @return
	 * @throws DocumentException 
	 * @throws IOException 
	 * @throws HttpException 
	 */
	public Map<String, List<String>> getApplication(String clusterId, String queueName, String userName, String applicationType) throws HttpException, IOException, DocumentException;

	/**
	 * 根据clusterId获取ResourceManager list
	 * @param clusterId
	 * @return
	 */
	public String[] getResourceManager(String clusterId);
}
