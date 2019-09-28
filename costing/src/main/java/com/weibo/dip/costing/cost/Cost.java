package com.weibo.dip.costing.cost;

import com.weibo.dip.costing.bean.DipCost;
import java.util.List;

/**
 * Created by yurun on 18/4/23.
 */
public interface Cost {

  String DIP_PRODUCT_UUID = "2012110217554677";

  double STORAGE_FACTOR = 0.2;
  double COMPUTE_FACTOR = 0.8;

  List<DipCost> cost(String year, String month);

}
