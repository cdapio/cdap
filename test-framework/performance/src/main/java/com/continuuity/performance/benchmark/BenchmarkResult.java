package com.continuuity.performance.benchmark;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Map;

/**
 * Used to return a summary of the benchmark metrics at the end of a run.
 */
public class BenchmarkResult {

  private final String name;
  private final String[] args;

  private long runtimeMillis;
  private final Map<String, GroupResult> groupResults = Maps.newHashMap();

  public BenchmarkResult(String name, String[] args) {
    this.name = name;
    this.args = args;
    this.runtimeMillis = 0L;
  }

  public void setRuntimeMillis(long runtimeMillis) {
    this.runtimeMillis = runtimeMillis;
  }

  public void add(GroupResult groupResult) {
    this.groupResults.put(groupResult.getName(), groupResult);
  }

  public String getName() {
    return name;
  }

  public String[] getArgs() {
    return args;
  }

  public long getRuntimeMillis() {
    return runtimeMillis;
  }

  public Collection<GroupResult> getGroupResults() {
    return groupResults.values();
  }

  public static class GroupResult {
    private final String name;
    private final int numInstances;
    private final Map<String, Long> metrics;

    public GroupResult(String groupName, int numInstances, Map<String, Long> metrics) {
      this.name = groupName;
      this.numInstances = numInstances;
      this.metrics = ImmutableMap.copyOf(metrics);
    }

    public String getName() {
      return name;
    }

    public int getNumInstances() {
      return numInstances;
    }

    public Map<String, Long> getMetrics() {
      return metrics;
    }
  }
}
