package com.weibo.dip.warehouse.udf;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Objects;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.Text;

/**
 * to_dirtime.
 *
 * @author yurun
 */
@Description(
    name = "to_dirtime",
    value =
      "_FUNC_(scheduleTime, hours) - "
          + "Increase the specified hours based on scheudle time(seconds)",
    extended = "Example: to_dirtime($SCHEDULETIME, -1), return: 2018073013"
)
public class ToDirTime extends GenericUDF {
  private ObjectInspectorConverters.Converter scheduleTimeConverter;
  private ObjectInspectorConverters.Converter hoursConverter;

  private Text result = new Text();

  private SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHH");

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (Objects.isNull(arguments)
        || arguments.length != 2
        || arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE
        || PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
                ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory())
            != PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP
        || arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE
        || PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
                ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory())
            != PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP) {
      throw new UDFArgumentException("Argument should be (scheduleTime, hours)");
    }

    scheduleTimeConverter =
        ObjectInspectorConverters.getConverter(
            arguments[0], PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    hoursConverter =
        ObjectInspectorConverters.getConverter(
            arguments[1], PrimitiveObjectInspectorFactory.javaIntObjectInspector);

    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    long scheduleTime = (Long) scheduleTimeConverter.convert(arguments[0].get());
    int hours = (Integer) hoursConverter.convert(arguments[1].get());

    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(scheduleTime * 1000);

    calendar.add(Calendar.HOUR_OF_DAY, hours);

    result.set(format.format(calendar.getTime()));

    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("to_dirtime", children);
  }
}
