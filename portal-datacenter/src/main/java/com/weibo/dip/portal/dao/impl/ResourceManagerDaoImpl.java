package com.weibo.dip.portal.dao.impl;

import com.weibo.dip.portal.dao.ResourceManagerDao;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.io.IOException;

@Repository("resourceManagerDao")
public class ResourceManagerDaoImpl implements ResourceManagerDao {
	private static final Logger LOGGER = LoggerFactory.getLogger(ResourceManagerDaoImpl.class);
	private HttpClient client = new HttpClient();
	// 是否交互url1和url2的标志，默认不交换：false
	private static boolean flag = false;
	
	@Override
	public String getInfoXML(String url1, String url2) throws HttpException, IOException {						
		String infoXml = null;
		GetMethod method = null;
		String tempUrl = null;
		LOGGER.info("flag: " + flag +" (if flag=true exchange url1 and url2, else remain unchange)" );
		
		// 若flag=true，则交换url1与url2
		if(flag){
			tempUrl = url2;
			url2 = url1;
			url1 = tempUrl;
		}
		LOGGER.info("Url1: " + url1);
		LOGGER.info("Url2: " + url2);
		
		// 获取activeRM返回的xml
		// 若url1为宕掉的standbyRM，则会抛出异常并连接url2
		try {
			method = new GetMethod(url1);
			method.addRequestHeader("Accept", "application/xml");
			client.executeMethod(method);
			infoXml = new String(method.getResponseBody(), CharEncoding.UTF_8);
			// 若url1为正常的standbyRM，则返回值不是xml格式的
			if(!infoXml.startsWith("<?xml")){
				// 若执行至此说明url2为activeRM，修改标志位
				flag = true;
				method = new GetMethod(url2);
				method.addRequestHeader("Accept", "application/xml");
				client.executeMethod(method);
				infoXml = new String(method.getResponseBody(), CharEncoding.UTF_8);
			}
		} catch(Exception e){
			// 若执行至此说明url2为activeRM，修改标志位
			flag = true;
			method = new GetMethod(url2);
			method.addRequestHeader("Accept", "application/xml");
			client.executeMethod(method);
			infoXml = new String(method.getResponseBody(), CharEncoding.UTF_8);
			
			LOGGER.error(ExceptionUtils.getFullStackTrace(e));
		}finally {
			method.releaseConnection();
		}	
		
		return infoXml;
	}

}
