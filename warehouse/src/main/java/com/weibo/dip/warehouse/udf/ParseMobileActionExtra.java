package com.weibo.dip.warehouse.udf;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

/**
 * parse_mobileaction_extra.
 *
 * @author yurun
 */
public class ParseMobileActionExtra extends GenericUDF {
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
    result.clear();

    DeferredObject argument = arguments[0];
    if (argument == null) {
      return result;
    }

    String extra = (String) argumentConverter.convert(argument.get());

    String[] parts = extra.split("\\|", -1);
    if (Objects.isNull(parts) || parts.length == 0) {
      return result;
    }

    for (String part : parts) {
      String[] words = part.split(":", -1);
      if (Objects.nonNull(words) && words.length == 2) {
        result.put(words[0], words[1]);
      }
    }

    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("parse_mobileaction_extra", children);
  }
}
