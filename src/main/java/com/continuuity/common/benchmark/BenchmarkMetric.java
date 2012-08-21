package com.continuuity.common.benchmark;

import com.continuuity.common.utils.ImmutablePair;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkMetric {

  private HashMap<String, AtomicLong> metrics = Maps.newHashMap();

  public void increment(String metric) {
    increment(metric, 1L);
  }
  public void increment(String metric, long delta) {
      AtomicLong counter = metrics.get(metric);
    if (counter == null) {
      synchronized (metrics) {
        counter = metrics.get(metric);
        if (counter == null) {
          counter = new AtomicLong(0L);
          metrics.put(metric, counter);
        }
      }
    }
    counter.addAndGet(delta);
  }

  public List<ImmutablePair<String, Long>> list() {
    return list(null);
  }

  public List<ImmutablePair<String, Long>> list(Set<String> keys) {
    if (keys == null) keys = metrics.keySet();
    List<ImmutablePair<String, Long>> result =
        new ArrayList<ImmutablePair<String, Long>>(keys.size());
    for (String key : keys) {
      AtomicLong counter = metrics.get(key);
      Long value = counter == null ? 0L : counter.longValue();
      result.add(new ImmutablePair<String, Long>(key, value));
    }
    return result;
  }

}
