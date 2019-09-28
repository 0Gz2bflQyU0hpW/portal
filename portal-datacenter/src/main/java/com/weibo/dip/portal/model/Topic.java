package com.weibo.dip.portal.model;

import java.util.List;

public class Topic {
	
	private String topicName;
	private String calTime;
	private String logSize;
	private List<Consumer> consumerList;
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
	
	public List<Consumer> getConsumerList() {
		return consumerList;
	}
	public void setConsumerList(List<Consumer> consumerList) {
		this.consumerList = consumerList;
	}
	@Override
	public String toString() {
		return "Topic [topicName=" + topicName + ", calTime=" + calTime + ", logSize=" + logSize + ", consumerList="
				+ consumerList + "]";
	}
	
}

