package com.weibo.dip.data.platform.commons.hive;

/**
 * Hive table column.
 *
 * @author yurun
 */
public class Column {
  private String name;
  private String type;
  private String comment;

  public Column() {}

  /**
   * Construct a instance.
   *
   * @param name column name
   * @param type column type
   * @param comment column comment
   */
  public Column(String name, String type, String comment) {
    this.name = name;
    this.type = type;
    this.comment = comment;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    if (!(obj instanceof Column)) {
      return false;
    }

    Column column = (Column) obj;

    return this.name.equals(column.name) && this.type.equals(column.type);
  }

  @Override
  public int hashCode() {
    return name.hashCode() * type.hashCode() + 1;
  }

  @Override
  public String toString() {
    return String.format("name: %s, type: %s, comment: %s", name, type, comment);
  }
}
