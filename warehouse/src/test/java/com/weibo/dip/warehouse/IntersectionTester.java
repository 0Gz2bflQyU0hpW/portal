package com.weibo.dip.warehouse;

import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Set;

/** @author yurun */
public class IntersectionTester {
  public static void main(String[] args) {
    Set<Integer> sets1 = Sets.newHashSet(0, 1, 2, 3, 4, 5);
    Set<Integer> sets2 = Sets.newHashSet(5, 6, 7, 8, 9);

    Sets.SetView<Integer> intersection = Sets.intersection(sets1, sets2);

    System.out.println(Arrays.toString(intersection.toArray()));

    Sets.SetView<Integer> diff1 = Sets.difference(sets1, intersection);

    System.out.println(Arrays.toString(diff1.toArray()));

    Sets.SetView<Integer> diff2 = Sets.difference(sets2, intersection);

    System.out.println(Arrays.toString(diff2.toArray()));
  }
}
