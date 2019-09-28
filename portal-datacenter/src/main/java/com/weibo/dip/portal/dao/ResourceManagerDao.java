package com.weibo.dip.portal.dao;

import org.apache.commons.httpclient.HttpException;

import java.io.IOException;

public interface ResourceManagerDao {
	
	/**
	 * 将url地址的xml文件转化为字符串
	 * @param url1 输入的地址
	 * @param url2 输入的地址
	 * @return xml内容
	 * @throws HttpException
	 * @throws IOException
	 */
	public String getInfoXML(String url1, String url2) throws HttpException, IOException;
}
