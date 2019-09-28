package com.weibo.dip.aliyundownload.domain;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.weibo.dip.aliyundownload.util.SignatureUtils;
import com.weibo.dip.data.platform.commons.util.HttpClientUtil;
import com.weibo.dip.data.platform.commons.util.UUIDUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.commons.lang3.time.FastTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Domain service.
 *
 * @author yurun
 */
public class DomainService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DomainService.class);

  private static final String ALIYUN_SERVICE = "https://cdn.aliyuncs.com/";

  private static final FastDateFormat YYYY_MM_DD_HH_MM_SS =
      FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
  private static final FastDateFormat ALIYUN_UTC_YYYY_MM_DD_T_HH_MM_SS_Z =
      FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ssZZ", FastTimeZone.getGmtTimeZone());

  private String accessKeyId;
  private String accessKeySecret;

  /**
   * Constaruct a DomainService instance.
   *
   * @param accessKeyId accessKeyId
   * @param accessKeySecret accessKeySecret
   */
  public DomainService(String accessKeyId, String accessKeySecret) {
    this.accessKeyId = accessKeyId;
    this.accessKeySecret = accessKeySecret;
  }

  /**
   * Get domains.
   *
   * @return domains
   * @throws Exception if http error
   */
  public List<Domain> getDomains() throws Exception {
    List<Domain> domains = new ArrayList<>();

    int pageNumber = 1;
    int totalCount = 0;

    while (true) {
      String action = "DescribeUserDomains";
      String format = "JSON";
      String version = "2014-11-11";
      String signatureMethod = "HMAC-SHA1";
      String signatureVersion = "1.0";

      Map<String, String> param = new HashMap<>();

      param.put("Action", action);
      param.put("PageNumber", String.valueOf(pageNumber));

      param.put("Format", format);
      param.put("Version", version);
      param.put("AccessKeyId", accessKeyId);
      param.put("SignatureMethod", signatureMethod);
      param.put("Timestamp", ALIYUN_UTC_YYYY_MM_DD_T_HH_MM_SS_Z.format(System.currentTimeMillis()));
      param.put("SignatureVersion", signatureVersion);
      param.put("SignatureNonce", UUIDUtil.getUUID());

      param.put("Signature", SignatureUtils.generate(param, accessKeySecret));

      String response = HttpClientUtil.doNativeGet(ALIYUN_SERVICE, param);

      LOGGER.debug("response: {}", response);

      JsonParser parser = new JsonParser();

      JsonObject result = parser.parse(response).getAsJsonObject();

      JsonArray domainArray =
          result.get("Domains").getAsJsonObject().get("PageData").getAsJsonArray();

      for (int index = 0; index < domainArray.size(); index++) {
        JsonObject domainJson = domainArray.get(index).getAsJsonObject();

        String cname = domainJson.getAsJsonPrimitive("Cname").getAsString();
        String description = domainJson.getAsJsonPrimitive("Description").getAsString();
        String cdnType = domainJson.getAsJsonPrimitive("CdnType").getAsString();
        String sourceType = domainJson.getAsJsonPrimitive("SourceType").getAsString();
        String domainStatus = domainJson.getAsJsonPrimitive("DomainStatus").getAsString();
        String sslProtocol = domainJson.getAsJsonPrimitive("SslProtocol").getAsString();
        String domainName = domainJson.getAsJsonPrimitive("DomainName").getAsString();

        JsonArray sourceArray = domainJson.getAsJsonObject("Sources").getAsJsonArray("Source");

        String[] sources = new String[sourceArray.size()];

        for (int sindex = 0; sindex < sourceArray.size(); sindex++) {
          sources[sindex] = sourceArray.get(sindex).getAsJsonPrimitive().getAsString();
        }

        String gmtModified = domainJson.getAsJsonPrimitive("GmtModified").getAsString();
        String sandbox = domainJson.getAsJsonPrimitive("Sandbox").getAsString();
        String gmtCreated = domainJson.getAsJsonPrimitive("GmtCreated").getAsString();

        Domain domain =
            new Domain(
                cname,
                description,
                cdnType,
                sourceType,
                domainStatus,
                sslProtocol,
                domainName,
                sources,
                gmtModified,
                sandbox,
                gmtCreated);

        domains.add(domain);

        totalCount++;
      }

      if (totalCount != result.getAsJsonPrimitive("TotalCount").getAsInt()) {
        pageNumber++;
      } else {
        break;
      }
    }

    return domains;
  }
}
