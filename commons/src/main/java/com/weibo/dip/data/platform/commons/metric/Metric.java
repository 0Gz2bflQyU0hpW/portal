package com.weibo.dip.data.platform.commons.metric;

import java.util.Date;

/** Created by yurun on 17/11/2. */
public class Metric<V> {
  private String name;
  private V value;

  private Metric(String name, V value) {
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public V getValue() {
    return value;
  }

  public void setValue(V value) {
    this.value = value;
  }

  public static Metric<String> newEntity(String name, String value) {
    return new Metric<>(name, value);
  }

  public static Metric<Long> newEntity(String name, long value) {
    return new Metric<>(name, value);
  }

  public static Metric<Float> newEntity(String name, float value) {
    return new Metric<>(name, value);
  }

  public static Metric<Date> newEntity(String name, Date value) {
    return new Metric<>(name, value);
  }
}
