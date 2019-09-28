package com.weibo.dip.costing.dao;

import com.weibo.dip.costing.bean.SummonResource;
import java.util.Date;
import java.util.List;

/**
 * Created by yurun on 18/4/25.
 */
public interface SummonDao {

  List<SummonResource> gets(Date beginTime, Date endTime);

}
