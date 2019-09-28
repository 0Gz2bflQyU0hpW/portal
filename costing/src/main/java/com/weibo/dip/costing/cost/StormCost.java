package com.weibo.dip.costing.cost;

import com.weibo.dip.costing.bean.DipCost;
import com.weibo.dip.costing.bean.Server.Role;
import com.weibo.dip.costing.bean.Server.Type;
import com.weibo.dip.costing.service.ProductService;
import com.weibo.dip.costing.service.ServerService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by yurun on 18/4/26.
 */
@Component
public class StormCost implements Cost {

  private static final Logger LOGGER = LoggerFactory.getLogger(StormCost.class);

  @Autowired
  private ProductService productService;

  @Autowired
  private ServerService serverService;

  @Override
  public List<DipCost> cost(String year, String month) {
    int server = serverService.servers();

    Map<String, Integer> exclusiveProductUuidToServers =
        serverService.servers(Role.STORM, Type.EXCLUSIVE);
    if (MapUtils.isEmpty(exclusiveProductUuidToServers)) {
      return null;
    }

    List<DipCost> costs = new ArrayList<>();

    exclusiveProductUuidToServers.entrySet().stream()
        .filter(entry -> {
          if (!productService.existByUuid(entry.getKey())) {
            LOGGER.warn("can't find product for uuid {}", entry.getKey());

            return false;
          }

          return true;
        })
        .forEach(
            entry -> {
              String productUuid = entry.getKey();
              int productServer = entry.getValue();

              DipCost cost = new DipCost();

              cost.setYear(year);
              cost.setMonth(month);
              cost.setProductName(productService.getByUuid(productUuid).getName());
              cost.setProductUuid(productUuid);
              cost.setCostQuota(productServer);
              cost.setQuotaUnit("servers");
              cost.setCostType("dip_storm_exclusive");
              cost.setPercent(productServer / 1.0 / server);
              cost.setDescs("");

              costs.add(cost);
            });

    return costs;
  }

}
