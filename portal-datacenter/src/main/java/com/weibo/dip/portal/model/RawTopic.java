package com.weibo.dip.portal.model;

public class RawTopic {
	String topicName;
	String calTime;
	String logSize;
	public String getTopicName() {
		return topicName;
	}
	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}
	public String getCalTime() {
		return calTime;
	}
	public void setCalTime(String calTime) {
		this.calTime = calTime;
	}
	public String getLogSize() {
		return logSize;
	}
	public void setLogSize(String logSize) {
		this.logSize = logSize;
	}
	@Override
	public String toString() {
		return "RawTopic [topicName=" + topicName + ", calTime=" + calTime + ", logSize=" + logSize + "]";
	}
	
	
}
