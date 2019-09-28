package com.weibo.dip.costing.cost;

import com.google.common.base.Preconditions;
import com.weibo.dip.costing.bean.DipCost;
import com.weibo.dip.costing.bean.Server.Role;
import com.weibo.dip.costing.bean.Server.Type;
import com.weibo.dip.costing.bean.SummonResource;
import com.weibo.dip.costing.service.ProductService;
import com.weibo.dip.costing.service.ServerService;
import com.weibo.dip.costing.service.SummonService;
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
public class SummonCost implements Cost {

  private static final Logger LOGGER = LoggerFactory.getLogger(SummonCost.class);

  @Autowired
  private ServerService serverService;

  @Autowired
  private ProductService productService;

  @Autowired
  private SummonService summonService;

  private List<DipCost> storageCost(String year, String month) {
    int server = serverService.servers();

    Map<String, Integer> productUuidToServers =
        serverService.servers(Role.SUMMON, Type.SHARE);
    Preconditions.checkState(
        MapUtils.isNotEmpty(productUuidToServers),
        "summon servers is empty");

    int summonServer = productUuidToServers.values().stream()
        .reduce((a, b) -> a + b).orElse(0);

    double summonServerPercent = summonServer / 1.0 / server;

    Date beginTime = DatetimeUtil.monthBegin(year, month);
    Date endTime = DatetimeUtil.monthEnd(year, month);

    List<SummonResource> summonResources = summonService.gets(beginTime, endTime);
    Preconditions.checkState(
        !CollectionUtils.isEmpty(summonResources), "summon resources is empty");

    Map<String, SummonResource> topicNameToKafkaResources = summonResources.stream()
        .filter(
            summonResource -> {
              if (!productService.existByUuid(summonResource.getProductUuid())) {
                LOGGER.warn("cat't find product info for {}", summonResource.getBusinessName());

                return false;
              }

              return true;
            })
        .collect(Collectors.toMap(
            SummonResource::getBusinessName,
            summonResource -> summonResource,
            (resourceA, resourceB) ->
                resourceA.getDisk() > resourceB.getDisk() ? resourceA : resourceB
        ));

    Map<String, Integer> productUuidToBytes = topicNameToKafkaResources.entrySet().stream()
        .collect(Collectors.toMap(
            entry -> entry.getValue().getProductUuid(),
            entry -> entry.getValue().getDisk(),
            (a, b) -> a + b
        ));

    int quantum = productUuidToBytes.values().stream().reduce((a, b) -> a + b).orElse(0);

    List<DipCost> costs = new ArrayList<>();

    productUuidToBytes.entrySet().stream()
        .forEach(entry -> {
          DipCost cost = new DipCost();

          cost.setYear(year);
          cost.setMonth(month);
          cost.setProductName(productService.getByUuid(entry.getKey()).getName());
          cost.setProductUuid(entry.getKey());
          cost.setCostQuota(entry.getValue());
          cost.setQuotaUnit("MB");
          cost.setCostType("dip_summon_storage");
          cost.setPercent(entry.getValue() / 1.0 / quantum * summonServerPercent * STORAGE_FACTOR);
          cost.setDescs("");

          costs.add(cost);
        });

    return costs;
  }

  private List<DipCost> computeCost(String year, String month) {
    int server = serverService.servers();

    Map<String, Integer> productUuidToServers =
        serverService.servers(Role.SUMMON, Type.SHARE);
    Preconditions.checkState(
        MapUtils.isNotEmpty(productUuidToServers),
        "summon servers is empty");

    int summonServer = productUuidToServers.values().stream()
        .reduce((a, b) -> a + b).orElse(0);

    double summonServerPercent = summonServer / 1.0 / server;

    Date beginTime = DatetimeUtil.monthBegin(year, month);
    Date endTime = DatetimeUtil.monthEnd(year, month);

    List<SummonResource> summonResources = summonService.gets(beginTime, endTime);
    Preconditions.checkState(
        !CollectionUtils.isEmpty(summonResources), "summon resources is empty");

    Map<String, SummonResource> topicNameToSummonResources = summonResources.stream()
        .filter(
            summonResource -> {
              if (!productService.existByUuid(summonResource.getProductUuid())) {
                LOGGER.warn("cat't find product info for {}", summonResource.getBusinessName());

                return false;
              }

              return true;
            })
        .collect(Collectors.toMap(
            SummonResource::getBusinessName,
            summonResource -> summonResource,
            (resourceA, resourceB) ->
                resourceA.getIndexQps() > resourceB.getIndexQps() ? resourceA : resourceB
        ));

    Map<String, Integer> productUuidToQps = topicNameToSummonResources.entrySet().stream()
        .collect(Collectors.toMap(
            entry -> entry.getValue().getProductUuid(),
            entry -> entry.getValue().getIndexQps(),
            (a, b) -> a + b
        ));

    int quantum = productUuidToQps.values().stream().reduce((a, b) -> a + b).orElse(0);

    List<DipCost> costs = new ArrayList<>();

    productUuidToQps.entrySet().stream()
        .forEach(entry -> {
          DipCost cost = new DipCost();

          cost.setYear(year);
          cost.setMonth(month);
          cost.setProductName(productService.getByUuid(entry.getKey()).getName());
          cost.setProductUuid(entry.getKey());
          cost.setCostQuota(entry.getValue());
          cost.setQuotaUnit("MB");
          cost.setCostType("dip_summon_indexqps");
          cost.setPercent(entry.getValue() / 1.0 / quantum * summonServerPercent * COMPUTE_FACTOR);
          cost.setDescs("");

          costs.add(cost);
        });

    return costs;
  }

  @Override
  public List<DipCost> cost(String year, String month) {
    List<DipCost> costs = new ArrayList<>();

    List<DipCost> storageCosts = storageCost(year, month);
    Preconditions.checkState(
        !CollectionUtils.isEmpty(storageCosts),
        "summon storage costs is empty");

    List<DipCost> computeCosts = computeCost(year, month);
    Preconditions.checkState(
        !CollectionUtils.isEmpty(computeCosts),
        "summon compute costs is empty");

    costs.addAll(storageCosts);
    costs.addAll(computeCosts);

    return costs;
  }

}
