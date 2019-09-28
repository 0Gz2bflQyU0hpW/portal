package com.weibo.dip.portal.util;

import org.springframework.http.HttpStatus;

public class HttpResponse {

	private int code;

	private String msg;

	private Object data;

	public HttpResponse() {

	}

	public HttpResponse(HttpStatus httpStatus) {
		code = httpStatus.value();

		msg = httpStatus.name();
	}

	public HttpResponse(HttpStatus httpStatus, Object data) {
		this(httpStatus);

		this.data = data;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}

	@Override
	public String toString() {
		return String.format("code: %s, msg: %s, data: %s", code, msg, data);
	}

}
