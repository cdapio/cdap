package com.continuuity.performance.benchmark;

import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class for benchmark metrics.
 */
public class BenchmarkMetric {

  private HashMap<String, AtomicLong> metrics = Maps.newHashMap();

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

  public Map<String, Long> list() {
    return list(null);
  }
  public Map<String, Long> list(Set<String> keys) {
    if (keys == null) {
      keys = metrics.keySet();
    }
    HashMap<String, Long> result = new HashMap<String, Long>(keys.size());
    for (String key : keys) {
      AtomicLong counter = metrics.get(key);
      Long value = counter == null ? 0L : counter.longValue();
      result.put(key, value);
    }
    return result;
  }

}
