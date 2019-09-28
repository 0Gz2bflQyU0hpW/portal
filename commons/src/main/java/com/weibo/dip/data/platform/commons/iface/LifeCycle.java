package com.weibo.dip.data.platform.commons.iface;

/**
 * Life cycle interface.
 *
 * @author yurun
 */
public interface LifeCycle {
  void start() throws Exception;

  void stop() throws Exception;
}
