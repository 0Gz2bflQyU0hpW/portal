package com.weibo.dip.streaming.bean.application;

import java.util.List;

/**
 * Application.
 *
 * @author yurun
 */
public class Application {
  private String id;
  private String name;
  private List<ApplicationAttempt> attempts;

  public Application() {}

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<ApplicationAttempt> getAttempts() {
    return attempts;
  }

  public void setAttempts(List<ApplicationAttempt> attempts) {
    this.attempts = attempts;
  }

  @Override
  public String toString() {
    return "Application{"
        + "id='"
        + id
        + '\''
        + ", name='"
        + name
        + '\''
        + ", attempts="
        + attempts
        + '}';
  }
}
