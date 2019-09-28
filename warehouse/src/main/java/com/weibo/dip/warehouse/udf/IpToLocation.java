package com.weibo.dip.warehouse.udf;

import com.weibo.dip.iplibrary.IpLibrary;
import com.weibo.dip.iplibrary.Location;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ip_to_location.
 *
 * @author yurun
 */
@Description(
    name = "ip_to_location",
    value =
        "_FUNC_(ip) - "
            + "Convert an ip address to country, province, city, district, isp, type, desc",
    extended = "Example: ip_to_location(ip), return: Map<String, String>")
public class IpToLocation extends GenericUDF {
  private static final Logger LOGGER = LoggerFactory.getLogger(IpToLocation.class);

  private static final String COUNTRY = "country";
  private static final String PROVINCE = "province";
  private static final String CITY = "city";
  private static final String DISTRICT = "district";
  private static final String ISP = "isp";
  private static final String TYPE = "type";
  private static final String DESC = "desc";

  private IpLibrary ipLibrary;

  private ObjectInspectorConverters.Converter argumentConverter;

  private Map<String, String> result = new HashMap<>();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (Objects.isNull(arguments)
        || arguments.length != 1
        || arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE
        || PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
                ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory())
            != PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP) {
      throw new UDFArgumentException("Argument should be a string type");
    }

    argumentConverter =
        ObjectInspectorConverters.getConverter(
            arguments[0], PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    return ObjectInspectorFactory.getStandardMapObjectInspector(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (Objects.isNull(ipLibrary)) {
      try {
        ipLibrary = new IpLibrary();
      } catch (Exception e) {
        throw new HiveException("iplibrary init error: " + e.getMessage());
      }
    }

    result.clear();

    DeferredObject argument = arguments[0];
    if (argument == null) {
      return result;
    }

    String ip = (String) argumentConverter.convert(argument.get());

    Location location = ipLibrary.getLocation(ip);
    if (Objects.nonNull(location)) {
      result.put(COUNTRY, location.getCountry());
      result.put(PROVINCE, location.getProvince());
      result.put(CITY, location.getProvince());
      result.put(DISTRICT, location.getDistrict());
      result.put(ISP, location.getIsp());
      result.put(TYPE, location.getType());
      result.put(DESC, location.getDesc());
    } else {
      result.put(COUNTRY, "");
      result.put(PROVINCE, "");
      result.put(CITY, "");
      result.put(DISTRICT, "");
      result.put(ISP, "");
      result.put(TYPE, "");
      result.put(DESC, "");
    }

    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("ip_to_location", children);
  }

  @Override
  public void close() throws IOException {
    if (Objects.nonNull(ipLibrary)) {
      ipLibrary.close();
    }
  }
}
