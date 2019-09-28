package com.weibo.dip.portal.model;

public class Consumer {
	private String consumerName;
	private String offsets;
	private String calTime;
	public String getConsumerName() {
		return consumerName;
	}
	public void setConsumerName(String consumerName) {
		this.consumerName = consumerName;
	}
	public String getOffsets() {
		return offsets;
	}
	public void setOffsets(String offsets) {
		this.offsets = offsets;
	}
	public String getCalTime() {
		return calTime;
	}
	public void setCalTime(String calTime) {
		this.calTime = calTime;
	}
	@Override
	public String toString() {
		return "Consumer [consumerName=" + consumerName + ", offsets=" + offsets + ", calTime=" + calTime + "]";
	}
	
}
