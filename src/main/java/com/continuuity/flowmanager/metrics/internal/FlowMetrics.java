package com.continuuity.flowmanager.metrics.internal;

import com.continuuity.flowmanager.metrics.CMetrics;
import com.continuuity.flowmanager.metrics.CMetricsContext;
import com.google.common.collect.Maps;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import java.util.Map;

/**
 *
 */
final class FlowMetrics implements CMetrics {
  private final Map<String, Counter> metrics = Maps.newConcurrentMap();
  private final CMetricsContext cmetric;
  private final String group;
  private final String type;

  public FlowMetrics(CMetricsContext cmetric, String group, String type) {
    this.cmetric = cmetric;
    this.group = group;
    this.type = type;
  }

  @Override
  public CMetricsContext getMetricsMeta() {
    return cmetric;
  }

  @Override
  public String getGroup() {
    return group;
  }

  @Override
  public String getType() {
    return type;
  }

  public void incr(String metric) {
    incr(metric, 1L);
  }

  public void incr(String metric, long l) {
    if(! metrics.containsKey(metric)) {
      MetricName name = new MetricName(group, type, cmetric.getName(metric), cmetric.getScope() );
      metrics.put(metric, Metrics.newCounter(name));
    }
    metrics.get(metric).inc(l);
  }

}
