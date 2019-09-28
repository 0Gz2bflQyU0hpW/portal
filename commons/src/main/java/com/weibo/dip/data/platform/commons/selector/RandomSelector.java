package com.weibo.dip.data.platform.commons.selector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Random selector.
 *
 * @param <T> select type
 */
public class RandomSelector<T> implements Selector<T> {
  private List<T> candidates = new ArrayList<>();

  private Random random = new Random();

  /**
   * Construct a instance.
   *
   * @param candidates select candidates
   */
  public RandomSelector(List<T> candidates) {
    if (candidates != null && !candidates.isEmpty()) {
      this.candidates.addAll(candidates);
    }
  }

  /**
   * Construct a instance.
   *
   * @param candidates select candidates
   */
  public RandomSelector(T[] candidates) {
    if (candidates != null && candidates.length > 0) {
      this.candidates.addAll(Arrays.asList(candidates));
    }
  }

  @Override
  public T select() {
    if (candidates.isEmpty()) {
      return null;
    }

    int value = Math.abs(random.nextInt());

    return candidates.get(value % candidates.size());
  }
}
