package com.weibo.dip.aliyundownload.log;

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.weibo.dip.aliyundownload.util.SignatureUtils;
import com.weibo.dip.data.platform.commons.util.HttpClientUtil;
import com.weibo.dip.data.platform.commons.util.UUIDUtil;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.commons.lang3.time.FastTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aliyun domain log service.
 *
 * @author yurun
 */
public class DomainLogDetailService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DomainLogDetailService.class);

  private static final String ALIYUN_SERVICE = "https://cdn.aliyuncs.com/";

  private static final FastDateFormat YYYY_MM_DD_HH_MM_SS =
      FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
  private static final FastDateFormat ALIYUN_UTC_YYYY_MM_DD_T_HH_MM_SS_Z =
      FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ssZZ", FastTimeZone.getGmtTimeZone());

  private String accessKeyId;
  private String accessKeySecret;

  private int threads;

  /**
   * Create DomainLogService.
   *
   * @param accessKeyId aliyun AccessKeyId
   * @param accessKeySecret aliyun AccessKeySecret
   * @param threads fetcher threads
   */
  public DomainLogDetailService(String accessKeyId, String accessKeySecret, int threads) {
    this.accessKeyId = accessKeyId;
    this.accessKeySecret = accessKeySecret;
    this.threads = threads;
  }

  private class DomainLogFetcher implements Callable<List<DomainLogDetail>> {

    private String domain;
    private String startTime;
    private String endTime;

    private String action = "DescribeCdnDomainLogs";
    private String format = "JSON";
    private String version = "2014-11-11";
    private String signatureMethod = "HMAC-SHA1";
    private String signatureVersion = "1.0";

    public DomainLogFetcher(String domain, String startTime, String endTime) {
      this.domain = domain;
      this.startTime = startTime;
      this.endTime = endTime;
    }

    @Override
    public List<DomainLogDetail> call() throws Exception {
      List<DomainLogDetail> logDetails = new ArrayList<>();

      int pageNumber = 1;
      int totalCount = 0;

      while (true) {
        LOGGER.debug("get logs for domain: {}, pageNumber: {}", domain, pageNumber);

        Map<String, String> param = new HashMap<>();

        param.put("Action", action);
        param.put("DomainName", domain);
        param.put("StartTime", startTime);
        param.put("EndTime", endTime);
        param.put("PageNumber", String.valueOf(pageNumber));

        param.put("Format", format);
        param.put("Version", version);
        param.put("AccessKeyId", accessKeyId);
        param.put("SignatureMethod", signatureMethod);
        param.put(
            "Timestamp", ALIYUN_UTC_YYYY_MM_DD_T_HH_MM_SS_Z.format(System.currentTimeMillis()));
        param.put("SignatureVersion", signatureVersion);
        param.put("SignatureNonce", UUIDUtil.getUUID());

        param.put("Signature", SignatureUtils.generate(param, accessKeySecret));

        String response = HttpClientUtil.doNativeGet(ALIYUN_SERVICE, param);

        LOGGER.debug("response: {}", response);

        JsonParser jsonParser = new JsonParser();

        JsonObject result = jsonParser.parse(response).getAsJsonObject();

        if (result.has("Code")) {
          String code = result.getAsJsonPrimitive("Code").getAsString();

          switch (code) {
            case "InvalidDomain.NotFound":
              LOGGER.debug("invalid domain {}: not found", domain);

              break;

            default:
              LOGGER.error(response);
          }

          break;
        }

        JsonArray domainLogDetails =
            result
                .getAsJsonObject()
                .get("DomainLogModel")
                .getAsJsonObject()
                .get("DomainLogDetails")
                .getAsJsonObject()
                .get("DomainLogDetail")
                .getAsJsonArray();

        for (int index = 0; index < domainLogDetails.size(); index++) {
          JsonObject domainLogDetail = domainLogDetails.get(index).getAsJsonObject();

          logDetails.add(
              new DomainLogDetail(
                  domain,
                  domainLogDetail.getAsJsonPrimitive("LogName").getAsString(),
                  domainLogDetail.getAsJsonPrimitive("LogPath").getAsString(),
                  domainLogDetail.getAsJsonPrimitive("StartTime").getAsString(),
                  domainLogDetail.getAsJsonPrimitive("EndTime").getAsString(),
                  domainLogDetail.getAsJsonPrimitive("LogSize").getAsLong()));

          totalCount++;
        }

        if (totalCount != result.getAsJsonPrimitive("TotalCount").getAsLong()) {
          pageNumber++;
        } else {
          break;
        }
      }

      if (!logDetails.isEmpty()) {
        LOGGER.debug(
            "domain: {}, startTime: {}, endTime: {}, logs: {}",
            domain,
            startTime,
            endTime,
            logDetails.size());
      }

      return logDetails;
    }
  }

  public List<DomainLogDetail> getLogs(List<String> domains, Date startTime, Date endTime)
      throws Exception {
    return getLogs(
        domains, YYYY_MM_DD_HH_MM_SS.format(startTime), YYYY_MM_DD_HH_MM_SS.format(endTime));
  }

  /**
   * Get logs for some domains.
   *
   * @param domains domain list
   * @param startTime start time(yyyy-MM-dd HH:mm:ss), include
   * @param endTime end time(yyyy-MM-dd HH:mm:ss), exclude
   * @return domain log list
   */
  public List<DomainLogDetail> getLogs(List<String> domains, String startTime, String endTime)
      throws Exception {
    Preconditions.checkState(CollectionUtils.isNotEmpty(domains), "domains can't be empty");
    Preconditions.checkState(StringUtils.isNotEmpty(startTime), "startTime can't be empty");
    Preconditions.checkState(StringUtils.isNotEmpty(endTime), "endTime can't be empty");

    String aliyunStartTime =
        ALIYUN_UTC_YYYY_MM_DD_T_HH_MM_SS_Z.format(YYYY_MM_DD_HH_MM_SS.parse(startTime));
    String aliyunEndTime =
        ALIYUN_UTC_YYYY_MM_DD_T_HH_MM_SS_Z.format(YYYY_MM_DD_HH_MM_SS.parse(endTime));

    List<DomainLogDetail> domainLogDetails = new ArrayList<>();

    ExecutorService executors = Executors.newFixedThreadPool(threads);

    List<Future<List<DomainLogDetail>>> futures = new ArrayList<>(domains.size());

    domains.forEach(
        domain ->
            futures.add(
                executors.submit(new DomainLogFetcher(domain, aliyunStartTime, aliyunEndTime))));

    futures.forEach(
        future -> {
          try {
            domainLogDetails.addAll(future.get());
          } catch (Exception e) {
            LOGGER.error("get domain log fetcher error: {}", ExceptionUtils.getStackTrace(e));
          }
        });

    executors.shutdown();

    return domainLogDetails;
  }
}
