package com.weibo.dip.costing.service.impl;

import com.weibo.dip.costing.bean.StreamingResource;
import com.weibo.dip.costing.dao.StreamingDao;
import com.weibo.dip.costing.service.StreamingService;
import java.util.Date;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by yurun on 18/4/25.
 */
@Service
public class StreamingServiceImpl implements StreamingService {

  @Autowired
  private StreamingDao streamingDao;

  @Override
  public List<StreamingResource> gets(Date beginTime, Date endTime) {
    return streamingDao.gets(beginTime, endTime);
  }

}
