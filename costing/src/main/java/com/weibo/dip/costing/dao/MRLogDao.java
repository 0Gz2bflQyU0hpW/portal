package com.weibo.dip.costing.dao;

import com.weibo.dip.costing.bean.MRLog;
import java.util.Date;
import java.util.List;

/**
 * Created by yurun on 18/4/24.
 */
public interface MRLogDao {

  List<MRLog> gets(Date beginTime, Date endTime);

}
