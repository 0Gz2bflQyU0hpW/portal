package com.weibo.dip.portal.util;


import com.google.gson.Gson;
import com.weibo.dip.portal.model.JsonBean;

/**
 * @author : dinglei create date : 2016-07-19
 * 
 *    T 为所返回data的类型
 * 
 */

public class JsonStausUtil {

	public static <T> String toJson(Integer status, String message, T data){
		
		JsonBean<T> jsonBean = new JsonBean<T>();
		if(status!=null){
			jsonBean.setStatus(status);
		}
		if(message!=null){
			jsonBean.setMessage(message);
		}
		if(data != null){
			jsonBean.setData(data);
		}
		
		Gson gson = new Gson();
		return gson.toJson(jsonBean);
	}
}



