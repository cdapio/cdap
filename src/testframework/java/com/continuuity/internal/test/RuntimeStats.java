package com.continuuity.internal.test;

import com.google.common.collect.Maps;
import scala.reflect.Print;

import java.io.PrintStream;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public final class RuntimeStats {

  private static ConcurrentMap<String, AtomicLong> counters = Maps.newConcurrentMap();

  public static void count(String name, int count) {
    System.out.println(name + " " + count);
    AtomicLong oldValue = counters.putIfAbsent(name, new AtomicLong(count));
    if (oldValue != null) {
      oldValue.addAndGet(count);
    }
  }

  public static void dump(PrintStream out) {
    out.println(counters);
  }

  private RuntimeStats() {
  }
}
