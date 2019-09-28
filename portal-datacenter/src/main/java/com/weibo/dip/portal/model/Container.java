package com.weibo.dip.portal.model;

public class Container implements Comparable<Container>{
	private String createTime;
	private String currentTime;
	private String container;
	private String address;
	private String clusterId;
	// 同top的RES,进程使用的、未被换出的物理内存大小
	private long rss;
	// 同top的VIRT,进程使用的虚拟内存总量,VIRT=SWAP+RES
	private long vms;
	// 同top的SHR,共享内存大小
	private long shared;
	private long timestamp;
	private long readCount;
	private long writeCount;
	private long readBytes;
	private long writeBytes;
	private double cpuPercent;
	public String getCreateTime() {
		return createTime;
	}
	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}
	public String getCurrentTime() {
		return currentTime;
	}
	public void setCurrentTime(String currentTime) {
		this.currentTime = currentTime;
	}
	public String getContainer() {
		return container;
	}
	public void setContainer(String container) {
		this.container = container;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public String getClusterId() {
		return clusterId;
	}
	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}	
	public long getRss() {
		return rss;
	}
	public void setRss(long rss) {
		this.rss = rss;
	}
	public long getVms() {
		return vms;
	}
	public void setVms(long vms) {
		this.vms = vms;
	}
	public long getShared() {
		return shared;
	}
	public void setShared(long shared) {
		this.shared = shared;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public long getReadCount() {
		return readCount;
	}
	public void setReadCount(long readCount) {
		this.readCount = readCount;
	}
	public long getWriteCount() {
		return writeCount;
	}
	public void setWriteCount(long writeCount) {
		this.writeCount = writeCount;
	}
	public long getReadBytes() {
		return readBytes;
	}
	public void setReadBytes(long readBytes) {
		this.readBytes = readBytes;
	}
	public long getWriteBytes() {
		return writeBytes;
	}
	public void setWriteBytes(long writeBytes) {
		this.writeBytes = writeBytes;
	}
	public double getCpuPercent() {
		return cpuPercent;
	}
	public void setCpuPercent(double cpuPercent) {
		this.cpuPercent = cpuPercent;
	}
	@Override
	public String toString() {
		return "Container [createTime=" + createTime + ", currentTime="
				+ currentTime + ", container=" + container + ", address="
				+ address + ", clusterId=" + clusterId + ", rss=" + rss
				+ ", vms=" + vms + ", shared=" + shared + ", timestamp="
				+ timestamp + ", readCount=" + readCount + ", writeCount="
				+ writeCount + ", readBytes=" + readBytes + ", writeBytes="
				+ writeBytes + ", cpuPercent=" + cpuPercent + "]";
	}
	@Override
	public int compareTo(Container o) {
		return o.getCurrentTime().compareTo(this.getCurrentTime());
	}		
	
	
}
