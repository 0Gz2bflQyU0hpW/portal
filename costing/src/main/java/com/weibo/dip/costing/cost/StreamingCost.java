package com.weibo.dip.costing.cost;

import com.google.common.base.Preconditions;
import com.weibo.dip.costing.bean.DipCost;
import com.weibo.dip.costing.bean.Server.Role;
import com.weibo.dip.costing.bean.Server.Type;
import com.weibo.dip.costing.bean.StreamingResource;
import com.weibo.dip.costing.service.ProductService;
import com.weibo.dip.costing.service.ServerService;
import com.weibo.dip.costing.service.StreamingService;
import com.weibo.dip.costing.util.DatetimeUtil;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * Created by yurun on 18/4/25.
 */
@Component
public class StreamingCost implements Cost {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingCost.class);

  @Autowired
  private ProductService productService;

  @Autowired
  private ServerService serverService;

  @Autowired
  private StreamingService streamingService;

  @Override
  public List<DipCost> cost(String year, String month) {
    int server = serverService.servers();

    /*
      Share
     */
    Map<String, Integer> shareProductUuidToServers =
        serverService.servers(Role.STREAMING, Type.SHARE);
    Preconditions.checkState(
        MapUtils.isNotEmpty(shareProductUuidToServers),
        "hadoop streaming servers is empty");

    int streamingServer = shareProductUuidToServers.values().stream()
        .reduce((a, b) -> a + b).orElse(0);

    double streamingServerPercent = streamingServer / 1.0 / server;

    Date beginTime = DatetimeUtil.monthBegin(year, month);
    Date endTime = DatetimeUtil.monthEnd(year, month);

    List<StreamingResource> streamingResources = streamingService.gets(beginTime, endTime);
    Preconditions.checkState(
        !CollectionUtils.isEmpty(streamingResources), "streaming resources is empty");

    Map<String, StreamingResource> appNameToStreamingResources = streamingResources.stream()
        .filter(
            streamingResource -> {
              if (!productService.existByUuid(streamingResource.getProductUuid())) {
                LOGGER.warn("cat't find product info for {}", streamingResource.getAppName());

                return false;
              }

              return true;
            })
        .collect(Collectors.toMap(
            StreamingResource::getAppName,
            streamingResource -> streamingResource,
            (resourceA, resourceB) ->
                resourceA.getServer() > resourceB.getServer() ? resourceA : resourceB
        ));

    Map<String, Integer> productUuidToServers = appNameToStreamingResources.entrySet().stream()
        .collect(Collectors.toMap(
            entry -> entry.getValue().getProductUuid(),
            entry -> entry.getValue().getServer(),
            (a, b) -> a + b
        ));

    int quantum = productUuidToServers.values().stream().reduce((a, b) -> a + b).orElse(0);

    List<DipCost> costs = new ArrayList<>();

    productUuidToServers.entrySet().stream()
        .forEach(entry -> {
          DipCost cost = new DipCost();

          cost.setYear(year);
          cost.setMonth(month);
          cost.setProductName(productService.getByUuid(entry.getKey()).getName());
          cost.setProductUuid(entry.getKey());
          cost.setCostQuota(entry.getValue());
          cost.setQuotaUnit("servers");
          cost.setCostType("dip_hadoop_streaming");
          cost.setPercent(entry.getValue() / 1.0 / quantum * streamingServerPercent);
          cost.setDescs("");

          costs.add(cost);
        });

    /*
      Exclusive
     */
    Map<String, Integer> exclusiveProductUuidToServers =
        serverService.servers(Role.STREAMING, Type.EXCLUSIVE);
    if (MapUtils.isNotEmpty(exclusiveProductUuidToServers)) {
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
                cost.setCostType("dip_hadoop_streaming_exclusive");
                cost.setPercent(productServer / 1.0 / server);
                cost.setDescs("");

                costs.add(cost);
              });
    }

    return costs;
  }

}
