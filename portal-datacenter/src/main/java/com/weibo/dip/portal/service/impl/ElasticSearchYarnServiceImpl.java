package com.weibo.dip.portal.service.impl;

import com.weibo.dip.portal.dao.ElasticSearchYarnDao;
import com.weibo.dip.portal.model.Container;
import com.weibo.dip.portal.service.ElasticSearchYarnService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.Map.Entry;

@Service("elasticSearchYarnService")
@Transactional
public class ElasticSearchYarnServiceImpl implements ElasticSearchYarnService {
	private final static Logger LOGGER = LoggerFactory.getLogger(ElasticSearchYarnServiceImpl.class);
	// 估计最大container个数
	private final static int MAX_CONTAINER = 80; 
	// 取最近NUM条记录
	private final static int  NUM = 60;
	
	@Autowired
	private ElasticSearchYarnDao elasticSearchYarnDao;

	/**
	 * 获得查询结果集
	 * @param clusterId
	 * @param applicationName
	 * @return
	 */
	public SearchHit[] searchHit(String clusterId, String applicationName) {
		SearchResponse scrollResp = elasticSearchYarnDao.searchScroll(clusterId, applicationName);	
		SearchHit[] searchHits = new SearchHit[0];
		SearchHit[] tempHits = null;		
		
		
		// 5分钟内同时运行80个container，需要采集 80*60=4800 个点
		while(searchHits.length < MAX_CONTAINER*NUM){
			tempHits =  scrollResp.getHits().getHits();
			searchHits = (SearchHit[]) ArrayUtils.addAll(searchHits, tempHits);			
			
			scrollResp = elasticSearchYarnDao.searchScrollResp(scrollResp.getScrollId());
		    //Break condition: No hits are returned
		    if (scrollResp.getHits().getHits().length == 0) {
		        break;
		    }		
		}
								
//		System.out.println("[ ElasticSearchYarnServiceImpl ] es中的记录：");
//		for(SearchHit hit : searchHits){System.out.println(hit.getSourceAsString());}
		LOGGER.info("get es records total number: " + scrollResp.getHits().getTotalHits());
		LOGGER.info("get es records real number: " + searchHits.length);
		
		return searchHits;
	}
	

	/**
	 * 查询所有的Application结果集
	 * @param matchKey
	 * @param matchValue
	 * @param timeSpan
	 * @return Set<String>
	 */
	@Deprecated
	public Set<String> searchApplication(String matchKey, String matchValue, String timeSpan) {
		SearchResponse scrollResp = elasticSearchYarnDao.searchScroll(matchKey, matchValue, timeSpan);
		SearchHit[] searchHits = scrollResp.getHits().getHits();
		Set<String> set = new HashSet<String>();
		String application;
		
		if(searchHits.length > 0){
			for(SearchHit hit : searchHits){
				application = (String)hit.getSource().get(matchKey);
				set.add(application);
			}
		}		
		return set;
	}

	@Override
	public Map<String, List<Container>> searchContainer(String clusterId, String applicationName) {
		long beginTime = System.currentTimeMillis();
		SearchHit[] searchHits = searchHit(clusterId, applicationName);		
		long endTime = System.currentTimeMillis();
		
		LOGGER.info("get data from elasticSearchYarnDao use time: " + (endTime - beginTime) + "ms");
		
		Map<String, List<Container>> containerMapList = new HashMap<String, List<Container>>();
		List<Container> containerList = new ArrayList<Container>();
		String containerName = null;
		String currentTime = null;
		String createTime = null;
		String address = null;
		long rss = 0;
		long vms = 0;
		long shared = 0;
		long readBytes = 0;
		long writeBytes = 0;
		long readCount = 0;
		long writeCount = 0;
		double cpuPercent = 0;
		long timestamp = 0;
		
		long now = System.currentTimeMillis()/1000;
		long fiveMinute = 60*5;
		long deviation = 10;
		long before = now - fiveMinute - deviation;
		
		if(searchHits != null && searchHits.length > 0){
			for(SearchHit hit : searchHits){				
				timestamp = Long.parseLong(hit.getSource().get("timestamp").toString()) ;	
				// 采集时的时间戳超过最近5分钟，则丢弃该记录
				if(timestamp < before){
					continue;
				}
				
				containerName = hit.getSource().get("container").toString();
				currentTime =  hit.getSource().get("current_time").toString();
				createTime =  hit.getSource().get("create_time").toString();
				address = hit.getSource().get("address").toString();
				rss = Long.parseLong(hit.getSource().get("rss").toString());
				vms = Long.parseLong(hit.getSource().get("vms").toString());
				shared = Long.parseLong(hit.getSource().get("shared").toString());
				readCount = Long.parseLong(hit.getSource().get("read_count").toString());
				writeCount = Long.parseLong(hit.getSource().get("write_count").toString());
				readBytes = Long.parseLong(hit.getSource().get("read_bytes").toString());
				writeBytes = Long.parseLong(hit.getSource().get("write_bytes").toString());				
				cpuPercent = (Double) hit.getSource().get("cpu_percent");								
				
				Container container = new Container();
				container.setContainer(containerName);
				container.setCpuPercent(cpuPercent);
				container.setCreateTime(createTime);
				container.setCurrentTime(currentTime);
				container.setAddress(address);
				container.setRss(rss);
				container.setTimestamp(timestamp);
				container.setVms(vms);
				container.setShared(shared);
				container.setReadBytes(readBytes);
				container.setWriteBytes(writeBytes);
				container.setReadCount(readCount);
				container.setWriteCount(writeCount);
								
//				System.out.println("从SearchHit取出 container名字：" + containerName);
//				System.out.println("es数据转化为container：\n" + container);
				
				// 代码优化后
				containerList =	containerMapList.get(containerName);				
				if(containerList != null){
					containerList.add(container);
				}else {
					containerList = new ArrayList<Container>();
					containerList.add(container);
					// key:contaner名称，value:关于这个container的所有记录
					containerMapList.put(containerName, containerList);
				}

//代码优化前				
//				if(containerMapList.containsKey(containerName)){					
//					containerList = containerMapList.get(containerName);
//					containerList.add(container);
//					containerMapList.put(containerName, containerList);
//				}else{
//					ArrayList<Container> containerInitList = new ArrayList<Container>();
//					containerInitList.add(container);
//					containerMapList.put(containerName, containerInitList);
//				}
			}
			
			
			for(Entry<String, List<Container>> entry : containerMapList.entrySet()){
				containerName = entry.getKey();
				containerList = entry.getValue();
				
//				从es中取得的数据可能大于规定的NUM个点，则取最新的NUM个点
				if(containerList.size() > NUM){
					containerList = containerList.subList(0, NUM);
				}				
				// 将containerList中的对象反转，即Container对象由旧到新
				Collections.reverse(containerList);
				containerMapList.put(containerName, containerList);
				
				LOGGER.debug("container Name: " + containerName + ", containerList size: " + containerList.size());
//				System.out.println("[ ElasticSearchYarnServiceImpl ] container名字：" + containerName);
//				System.out.println("[ ElasticSearchYarnServiceImpl ] containerList按照container的currentTime排序后：" );
//				for(Container c : containerList)System.out.println(c);
			}			
		}
		
		return containerMapList;
	}

	@Override
	public Map<String, List<Container>> searchContainer(String clusterId, String applicationName, String timeSpan) {
		long beginTime = System.currentTimeMillis();
		SearchResponse sr = elasticSearchYarnDao.searchScroll(clusterId, applicationName, timeSpan);
		long endTime = System.currentTimeMillis();
		
		LOGGER.info("get data from elasticSearchYarnDao use time: " + (endTime - beginTime) + "ms");
		LOGGER.info("get es records total number: " + sr.getHits().getTotalHits());
		
		Map<String, List<Container>> containerMapList = new HashMap<String, List<Container>>();
		List<Container> containerList = new ArrayList<Container>();
		String containerName = null;
		String address = null;
		String createTime = null;
		
		Terms containerAggregation = sr.getAggregations().get("containerAggregation");
		for (Terms.Bucket containerEntry : containerAggregation.getBuckets()) {

			containerName = containerEntry.getKey().toString();      // Term		
			
//		    long containerDocCount = containerEntry.getDocCount(); // Doc count		    		    		    		    
//		    LOGGER.info("containerName [{}], containerDocCount [{}]", containerName, containerDocCount);
		    
		    Terms addressAggregation = containerEntry.getAggregations().get("addressAggregation");
			for (Terms.Bucket addressEntry : addressAggregation.getBuckets()) {
				address = addressEntry.getKey().toString();      // Term	
				
//			    long addressDocCount = addressEntry.getDocCount(); // Doc count			    		    
//			    LOGGER.info("address [{}], addressDocCount [{}]", address, addressDocCount);
			}   
			    
		    Terms creatTimeAggregation = containerEntry.getAggregations().get("creatTimeAggregation");
			for (Terms.Bucket createTimeEntry : creatTimeAggregation.getBuckets()) {
				createTime = createTimeEntry.getKey().toString();      // Term		
				
//			    long createTimeDocCount = createTimeEntry.getDocCount(); // Doc count			    		    
//			    LOGGER.info("creatTime [{}], addressDocCount [{}]", createTime, createTimeDocCount);	
			}		    					    		    
		    
			Range rangAggregation = containerEntry.getAggregations().get("rangAggregation");
			
			// For each entry
			for (Range.Bucket rangEntry : rangAggregation.getBuckets()) {
//			    String key = rangEntry.getKeyAsString();             // Range as key
//			    Number from = (Number) rangEntry.getFrom();          // Bucket from
//			    Number to = (Number) rangEntry.getTo();              // Bucket to
//			    long docCount = rangEntry.getDocCount();    // Doc count
//			    LOGGER.info("key [{}], from [{}], to [{}], doc_count [{}]", key, from, to, docCount);			    
		
			    Histogram timeAggregation = rangEntry.getAggregations().get("timeAggregation");		    		    
				for (Histogram.Bucket timeEntry : timeAggregation.getBuckets()) {
				    long timestamp = (Long)timeEntry.getKey();       // Key			 
//				    long docCount = timeEntry.getDocCount();    // Doc count
				    
				    Avg avg_rss = timeEntry.getAggregations().get("avg_rss");
				    Avg avg_vms = timeEntry.getAggregations().get("avg_vms");
				    Avg avg_shared = timeEntry.getAggregations().get("avg_shared");
				    Avg avg_readBytes = timeEntry.getAggregations().get("avg_readBytes");
				    Avg avg_writeBytes = timeEntry.getAggregations().get("avg_writeBytes");
				    Avg avg_readCount = timeEntry.getAggregations().get("avg_readCount");
				    Avg avg_writeCount = timeEntry.getAggregations().get("avg_writeCount");
				    Avg avg_cpuPercent = timeEntry.getAggregations().get("avg_cpuPercent");
				    
				    long rss = (long) avg_rss.getValue();
					long vms = (long) avg_vms.getValue();
					long shared = (long) avg_shared.getValue();
					long readBytes = (long) avg_readBytes.getValue();
					long writeBytes = (long) avg_writeBytes.getValue();
					long readCount = (long) avg_readCount.getValue();
					long writeCount = (long) avg_writeCount.getValue();
					double cpuPercent = avg_cpuPercent.getValue();
	
					String currentTime = Long.toString(timestamp);
					
				    
	//			    LOGGER.info("timestamp [{}], currentTime [{}], doc_count [{}], rss [{}], vms [{}], shared [{}], "
	//			    		+ "readBytes [{}], writeBytes [{}], readCount [{}], writeCount [{}], cpuPercent [{}]", 
	//			    		timestamp, currentTime, docCount, rss, vms, shared, readBytes, writeBytes, readCount, writeCount, cpuPercent);
				    
				    // 将聚合后的数据存入container
				    Container container = new Container();
					container.setContainer(containerName);
					container.setCpuPercent(cpuPercent);
					container.setCreateTime(createTime);
					container.setCurrentTime(currentTime);
					container.setAddress(address);
					container.setRss(rss);
					container.setTimestamp(timestamp);
					container.setVms(vms);
					container.setShared(shared);
					container.setReadBytes(readBytes);
					container.setWriteBytes(writeBytes);
					container.setReadCount(readCount);
					container.setWriteCount(writeCount);
									
					containerList =	containerMapList.get(containerName);				
					if(!CollectionUtils.isEmpty(containerList)){
						containerList.add(container);
					}else {
						containerList = new ArrayList<Container>();
						containerList.add(container);
						// key:contaner名称，value:关于这个container的所有记录
						containerMapList.put(containerName, containerList);
					}
				}
			    
			    
			}
			
		    
		    
			    		    		    
		}
			
		
		return containerMapList;
	}
	
	
}
