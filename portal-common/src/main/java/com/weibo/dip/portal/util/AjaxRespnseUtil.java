package com.weibo.dip.portal.util;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author : dinglei create date : 2016-07-14
 */
public class AjaxRespnseUtil {
	//JSON
	public static void renderJson(HttpServletResponse response,String text) throws IOException{
		render(response, "application/json;charset=UTF-8", text);
	}
	//xml
	public static void renderXml(HttpServletResponse response,String text) throws IOException{
		render(response, "text/xml;charset=UTF-8", text);
	}
	//text
	public static void renderText(HttpServletResponse response,String text) throws IOException{
		render(response, "text/plain;charset=UTF-8", text);
	}
	
	public static void render(HttpServletResponse response,String contentType,String text) throws IOException{
		response.setContentType(contentType);
		response.getWriter().write(text);
	}
	
}
