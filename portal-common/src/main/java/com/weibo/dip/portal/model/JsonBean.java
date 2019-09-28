package com.weibo.dip.portal.model;

public class JsonBean <T> {
    private Integer status=0;
	private String message="";
	private T data;
	public Integer getStatus() {
		return status;
	}
	public void setStatus(Integer status) {
		this.status = status;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public T getData() {
		return data;
	}
	public void setData(T data) {
		this.data =  data;
	}
}