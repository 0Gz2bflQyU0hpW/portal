package com.weibo.dip.platform.model;

import java.util.Arrays;
import java.util.List;

public enum StateEnum {
  STARTPOOLFAILD(-20, "运行超时"),
  YARNFAILD(-50, "running error"),

  WAITSTART(10, "等待启动"),
  STARTPOOL(20, "启动线程"),
  SUBMITSUCCESS(30, "提交成功"),
  YARNGET(40, "已在yarn上"),
  YARNSUCCESS(50, "running"),
  WAITRESTART(60, "等待重启"),
  RESTARTING(70, "重启中"),
  WAITKILL(80, "等待kill"),
  KILLING(90, "正在停止"),
  KILLED(100, "停止成功");

  private int number;
  private String describe;

  StateEnum(int number, String name) {
    this.number = number;
    this.describe = name;
  }

  public int getNumber() {
    return number;
  }

  /** get yarn previous list. */
  public static List<Integer> getYarnPreviousStates() {
    return Arrays.asList(
        STARTPOOL.getNumber(),
        SUBMITSUCCESS.getNumber(),
        YARNGET.getNumber(),
        YARNSUCCESS.getNumber());
  }

  /** get kill previous list. */
  public static List<Integer> getKillPreviousStates() {
    return Arrays.asList(
        WAITSTART.getNumber(),
        STARTPOOL.getNumber(),
        SUBMITSUCCESS.getNumber(),
        YARNGET.getNumber(),
        YARNSUCCESS.getNumber());
  }

  @Override
  public String toString() {
    return number + ":" + describe;
  }
}
