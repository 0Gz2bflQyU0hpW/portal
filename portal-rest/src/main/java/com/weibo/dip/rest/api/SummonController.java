package com.weibo.dip.rest.api;

import com.google.gson.reflect.TypeToken;
import com.weibo.dip.data.platform.commons.GlobalResourceManager;
import com.weibo.dip.data.platform.commons.Symbols;
import com.weibo.dip.data.platform.commons.record.Record;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.kafka.KafkaWriter;
import com.weibo.dip.rest.Conf;
import com.weibo.dip.rest.bean.Result;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Summon controller.
 *
 * @author yurun
 */
@RestController
@RequestMapping("/summon")
public class SummonController {

  private static final Logger LOGGER = LoggerFactory.getLogger(SummonController.class);

  private static final String BUSINESS = "business";
  private static final String TIMESTAMP = "timestamp";

  private static final String STR = "string";
  private static final String LONG = "long";
  private static final String FLOAT = "float";

  private static final Pattern SUMMON_ENTRY_PATTERN =
      Pattern.compile("^([^|]+)\\|([^|]+)\\|(" + STR + "|" + LONG + "|" + FLOAT + ")$");

  private static final Type RECORDS_GSON_TYPE = new TypeToken<List<Record>>() {}.getType();

  @Autowired private Environment env;

  private KafkaWriter summonWriter;

  public SummonController() {
    summonWriter = GlobalResourceManager.get(KafkaWriter.class);
  }

  /**
   * Write record to summon kafka topic.
   *
   * @param business summonn business
   * @param timestamp summon timestamp
   * @param parameters dimensions and metrics
   * @return ok or error
   */
  @Deprecated
  @RequestMapping(value = "write", method = RequestMethod.POST)
  public Result write(
      @RequestParam String business,
      @RequestParam long timestamp,
      @RequestParam String parameters) {
    if (StringUtils.isEmpty(business)) {
      return new Result(HttpStatus.BAD_REQUEST, "business is empty");
    }

    if (StringUtils.isEmpty(parameters)) {
      return new Result(HttpStatus.BAD_REQUEST, "parameters is empty");
    }

    try {
      Map<String, Object> pairs = new HashMap<>();

      String[] entries = parameters.split(Symbols.SEMICOLON);

      for (String entry : entries) {
        if (StringUtils.isEmpty(entry)) {
          continue;
        }

        Matcher matcher = SUMMON_ENTRY_PATTERN.matcher(entry);
        if (!matcher.matches()) {
          pairs.clear();

          break;
        }

        String name = matcher.group(1);
        String value = matcher.group(2);
        String type = matcher.group(3);

        switch (type) {
          case STR:
            pairs.put(name, value);
            break;

          case LONG:
            pairs.put(name, Long.valueOf(value));
            break;

          case FLOAT:
            pairs.put(name, Float.valueOf(value));
            break;

          default:
            pairs.put(name, value);
        }
      }

      if (MapUtils.isEmpty(pairs)) {
        return new Result(
            HttpStatus.BAD_REQUEST,
            "parameters must match key1|abc|string;key2|123|long;key3|0.456|float");
      }

      pairs.put(BUSINESS, business);
      pairs.put(TIMESTAMP, timestamp);

      summonWriter.write(env.getProperty(Conf.SUMMON_TOPIC), GsonUtil.toJson(pairs));

      return new Result(HttpStatus.OK);
    } catch (Exception e) {
      LOGGER.error("summon write error: {}", ExceptionUtils.getFullStackTrace(e));

      return new Result(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Write records to summon kafka topic.
   *
   * @param records one or more records.
   * @return ok or error
   * @see Record
   */
  @RequestMapping(value = "writes", method = RequestMethod.POST)
  public Result writes(@RequestParam String records) {
    if (StringUtils.isEmpty(records)) {
      return new Result(HttpStatus.BAD_REQUEST, "parameter records is empty");
    }

    try {
      List<Record> rds = GsonUtil.fromJson(records, RECORDS_GSON_TYPE);

      for (Record rd : rds) {
        Map<String, Object> pairs = new HashMap<>();

        pairs.put(BUSINESS, rd.getBusiness());
        pairs.put(TIMESTAMP, rd.getTimestamp());
        rd.getDimensions().forEach(pairs::put);
        rd.getMetrics().forEach(pairs::put);

        summonWriter.write(env.getProperty(Conf.SUMMON_TOPIC), GsonUtil.toJson(pairs));
      }

      return new Result((HttpStatus.OK));
    } catch (Exception e) {
      LOGGER.error("summon write error: {}", ExceptionUtils.getFullStackTrace(e));

      return new Result(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }
}
