package com.weibo.dip.warehouse.serde;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonStringSerde extends AbstractSerDe {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonStringSerde.class);

  private static final String COLUMN_EXTRA = "extra";

  private int numColumns;

  private List<String> columnNames;

  private StructObjectInspector rowOi;

  private List<String> row;

  private JsonParser parser = new JsonParser();

  private Map<String, String> alias;

  private boolean useColumnRegex = false;

  private Pattern columnRegexPattern;

  @Override
  public void initialize(@Nullable Configuration configuration, Properties tbl)
      throws SerDeException {
    String columnNameProperty = tbl.getProperty("columns");
    String columnTypeProperty = tbl.getProperty("columns.types");

    columnNames = Arrays.asList(columnNameProperty.split(","));
    List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

    if (!columnNames.get(columnNames.size() - 1).equals(COLUMN_EXTRA)) {
      throw new SerDeException(
          this.getClass().getName() + "'s last column name must be '" + COLUMN_EXTRA + "'");
    }

    assert columnNames.size() == columnTypes.size();

    numColumns = columnNames.size();
    List<ObjectInspector> columnOIs = new ArrayList<>(columnNames.size());

    for (int c = 0; c < numColumns; ++c) {
      TypeInfo typeInfo = columnTypes.get(c);
      if (!(typeInfo instanceof PrimitiveTypeInfo)) {
        throw new SerDeException(
            this.getClass().getName()
                + " doesn't allow column ["
                + c
                + "] named "
                + columnNames.get(c)
                + " with type "
                + columnTypes.get(c));
      }

      PrimitiveTypeInfo pti = (PrimitiveTypeInfo) columnTypes.get(c);

      if (!(pti.getPrimitiveCategory().equals(PrimitiveCategory.STRING))) {
        throw new SerDeException(
            this.getClass().getName()
                + " doesn't allow column ["
                + c
                + "] named "
                + columnNames.get(c)
                + " with type "
                + columnTypes.get(c));
      }

      AbstractPrimitiveJavaObjectInspector oi =
          PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(pti);

      columnOIs.add(oi);
    }

    rowOi = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);

    row = new ArrayList<>(numColumns);
    for (int c = 0; c < numColumns; ++c) {
      row.add(null);
    }

    alias = new HashMap<>();

    String columnAlias = tbl.getProperty("column.alias");
    if (columnAlias != null && !columnAlias.isEmpty()) {
      String[] pairs = columnAlias.split(",", -1);

      for (String pair : pairs) {
        String[] words = pair.split(":", -1);
        if (words.length == 2) {
          alias.put(words[0], words[1]);
        }
      }
    }

    String columnRegex = tbl.getProperty("column.regex");
    if (columnRegex != null && !columnRegex.isEmpty()) {
      useColumnRegex = true;

      try {
        columnRegexPattern = Pattern.compile(columnRegex);
      } catch (PatternSyntaxException e) {
        throw new SerDeException(e.getMessage());
      }
    }
  }

  @Override
  public Object deserialize(Writable writable) {
    Text rowText = (Text) writable;

    String line = rowText.toString();
    if (line == null || line.isEmpty()) {
      return null;
    }

    int c = 0;

    if (useColumnRegex) {
      Matcher matcher = columnRegexPattern.matcher(line);
      if (!matcher.matches()) {
        return null;
      }

      int group = matcher.groupCount();

      for (int g = 1; g < group; g++) {
        row.set(c++, matcher.group(g));
      }

      line = matcher.group(group);
    }

    JsonElement jsonElement;

    try {
      jsonElement = parser.parse(line);
    } catch (JsonSyntaxException e) {
      return null;
    }

    if (!jsonElement.isJsonObject()) {
      return null;
    }

    JsonObject jsonObject = jsonElement.getAsJsonObject();

    for (; c < numColumns - 1; c++) {
      String columnName = columnNames.get(c);

      if (alias.containsKey(columnName)) {
        columnName = alias.get(columnName);
      }

      if (jsonObject.has(columnName)) {
        JsonElement property = jsonObject.remove(columnName);

        if (property.isJsonPrimitive()) {
          row.set(c, property.getAsJsonPrimitive().getAsString());
        } else {
          row.set(c, property.toString());
        }
      } else {
        row.set(c, null);
      }
    }

    row.set(numColumns - 1, jsonObject.toString());

    return row;
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return rowOi;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return null;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) {
    throw new UnsupportedOperationException(
        "JsonString SerDe doesn't support the serialize() method");
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }
}
