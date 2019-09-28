package com.weibo.dip.scheduler.bean;

/**
 * Application state.
 *
 * @author yurun
 */
public enum ApplicationState {
  QUEUED,
  PENDING,
  RUNNING,
  SUCCESS,
  FAILED,
  KILLED
}
