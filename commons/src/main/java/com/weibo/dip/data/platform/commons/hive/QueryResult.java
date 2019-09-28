package com.weibo.dip.data.platform.commons.hive;

import java.util.Arrays;
import java.util.List;

/**
 * Query result.
 *
 * @author yurun
 */
public class QueryResult {

  public enum Type {
    ALL,
    PART
  }

  private Type type;

  private int columns;

  private String[] columnNames;
  private String[] columnTypes;

  private List<List<Object>> datas;

  public QueryResult() {}

  /**
   * Construct a instance.
   *
   * @param columns column count
   * @param columnNames column names
   * @param columnTypes column types
   * @param datas rows
   */
  public QueryResult(
      int columns, String[] columnNames, String[] columnTypes, List<List<Object>> datas) {
    this.columns = columns;
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.datas = datas;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public int getColumns() {
    return columns;
  }

  public void setColumns(int columns) {
    this.columns = columns;
  }

  public String[] getColumnNames() {
    return columnNames;
  }

  public void setColumnNames(String[] columnNames) {
    this.columnNames = columnNames;
  }

  public String[] getColumnTypes() {
    return columnTypes;
  }

  public void setColumnTypes(String[] columnTypes) {
    this.columnTypes = columnTypes;
  }

  public List<List<Object>> getDatas() {
    return datas;
  }

  public void setDatas(List<List<Object>> datas) {
    this.datas = datas;
  }

  @Override
  public String toString() {
    return "QueryResult{"
        + "type="
        + type
        + ", columns="
        + columns
        + ", columnNames="
        + Arrays.toString(columnNames)
        + ", columnTypes="
        + Arrays.toString(columnTypes)
        + ", datas="
        + datas
        + '}';
  }
}
