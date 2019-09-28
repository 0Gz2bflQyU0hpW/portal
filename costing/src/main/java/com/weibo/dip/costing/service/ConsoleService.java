package com.weibo.dip.costing.service;

import com.weibo.dip.costing.bean.Dataset;
import java.util.List;
import org.springframework.dao.DataAccessException;

/**
 * Created by yurun on 18/4/18.
 */
public interface ConsoleService {

  List<Dataset> gets() throws DataAccessException;

}
