package com.weibo.dip.costing.cost;

import com.google.common.base.Preconditions;
import com.weibo.dip.costing.bean.DipCost;
import com.weibo.dip.costing.bean.KafkaResource;
import com.weibo.dip.costing.bean.Server.Role;
import com.weibo.dip.costing.bean.Server.Type;
import com.weibo.dip.costing.service.KafkaService;
import com.weibo.dip.costing.service.ProductService;
import com.weibo.dip.costing.service.ServerService;
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
public class KafkaCost implements Cost {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaCost.class);

  @Autowired
  private ProductService productService;

  @Autowired
  private ServerService serverService;

  @Autowired
  private KafkaService kafkaService;

  @Override
  public List<DipCost> cost(String year, String month) {
    int server = serverService.servers();

    /*
      Share
     */
    Map<String, Integer> shareProductUuidToServers =
        serverService.servers(Role.KAFKA, Type.SHARE);
    Preconditions.checkState(
        MapUtils.isNotEmpty(shareProductUuidToServers),
        "kafka servers is empty");

    int kafkaServer = shareProductUuidToServers.values().stream()
        .reduce((a, b) -> a + b).orElse(0);

    double kafkaServerPercent = kafkaServer / 1.0 / server;

    Date beginTime = DatetimeUtil.monthBegin(year, month);
    Date endTime = DatetimeUtil.monthEnd(year, month);

    List<KafkaResource> kafkaResources = kafkaService.gets(beginTime, endTime);
    Preconditions.checkState(
        !CollectionUtils.isEmpty(kafkaResources), "kafka resources is empty");

    Map<String, KafkaResource> topicNameToKafkaResources = kafkaResources.stream()
        .filter(
            streamingResource -> {
              if (!productService.existByUuid(streamingResource.getProductUuid())) {
                LOGGER.warn("cat't find product info for {}", streamingResource.getTopicName());

                return false;
              }

              return true;
            })
        .collect(Collectors.toMap(
            KafkaResource::getTopicName,
            kafkaResource -> kafkaResource,
            (resourceA, resourceB) ->
                resourceA.getPeakBytesout() > resourceB.getPeakBytesout() ? resourceA : resourceB
        ));

    Map<String, Double> productUuidToBytes = topicNameToKafkaResources.entrySet().stream()
        .collect(Collectors.toMap(
            entry -> entry.getValue().getProductUuid(),
            entry -> entry.getValue().getPeakBytesout(),
            (a, b) -> a + b
        ));

    double quantum = productUuidToBytes.values().stream().reduce((a, b) -> a + b).orElse(0.0);

    List<DipCost> costs = new ArrayList<>();

    productUuidToBytes.entrySet().stream()
        .forEach(entry -> {
          DipCost cost = new DipCost();

          cost.setYear(year);
          cost.setMonth(month);
          cost.setProductName(productService.getByUuid(entry.getKey()).getName());
          cost.setProductUuid(entry.getKey());
          cost.setCostQuota(entry.getValue());
          cost.setQuotaUnit("B");
          cost.setCostType("dip_kafka");
          cost.setPercent(entry.getValue() / quantum * kafkaServerPercent);
          cost.setDescs("");

          costs.add(cost);
        });

    /*
      Exclusive
     */
    Map<String, Integer> exclusiveProductUuidToServers =
        serverService.servers(Role.KAFKA, Type.EXCLUSIVE);
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
                cost.setQuotaUnit("B");
                cost.setCostType("dip_kafka_exclusive");
                cost.setPercent(productServer / 1.0 / server);
                cost.setDescs("");

                costs.add(cost);
              });
    }

    return costs;
  }

}
