package com.weibo.dip.data.platform.commons.selector;

/**
 * Selector.
 *
 * @author yurun
 * @param <T> select type
 */
public interface Selector<T> {
  T select();
}
