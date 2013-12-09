package com.continuuity.common.metrics;

import com.google.common.collect.Maps;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A simple API for translating the metrics collected for specific
 * groups in the systems into yammer metrics. It's more like an
 * adapter that simplifies our process.
 *
 * It current supports {@link Counter} and {@link Gauge}.
 */
public class CMetrics {
  /**
   * Type of metric the {@link CMetrics} is responsible for collecting.
   */
  private final MetricType metricType;

  /**
   * Mapping of metric name to it's equivalent yammer metric counters.
   */
  private Map<String, Counter> counters;

  /**
   * Mapping of metric name to it's equivalent yammer gauge.
   */
  private Map<String, Gauge> gauges;

  /**
   * Mapping of metric name to it's equivalent meters measuring rates.
   */
  private Map<String, Meter> meters;

  /**
   * Mapping of metric name to their histograms being tracked.
   */
  private Map<String, Histogram> histograms;

  /**
   * Specifies the group the metrics are collected for.
   */
  private final String metricGroup;

  /**
   * Constructor specifying the type of metric as specified by
   * the {@link MetricType} and the group the metrics belong to.
   *
   * @param metricType collected
   * @param metricGroup the metrics being collected belong to.
   */
  public CMetrics(MetricType metricType, String metricGroup)
    throws IllegalArgumentException {

    // if a metric type is user or system flow metric then metric
    // group has be valid.
    if (metricType == MetricType.FlowSystem ||
        metricType == MetricType.FlowUser) {
      if (metricGroup == null || metricGroup.isEmpty()) {
        throw new IllegalArgumentException("Metric group cannot be empty or null " +
                                             "for a flow metric.");
      }
    }

    this.metricType = metricType;
    this.metricGroup = metricGroup;
    this.counters = Maps.newConcurrentMap();
    this.gauges = Maps.newConcurrentMap();
    this.meters = Maps.newConcurrentMap();
    this.histograms = Maps.newConcurrentMap();
  }

  public CMetrics(MetricType metricType) throws IllegalArgumentException {
    this(metricType, "");
  }

  /**
   * Increments a given metric counter. It's a specialization of a gauge
   * that allows to increment or decrement a value associated with the
   * counter metric.
   *
   * @param metricName name of the metric.
   * @param value to increment the metric by.
   */
  public void counter(String metricName, long value) {
    counter(null, metricName, value);
  }

  /**
   * Increments a given metric counter. It's a specialization of a gauge
   * that allows to increment or decrement a value associated with the
   * counter metric.
   *
   * @param scope of the metric being measured.
   * @param metricName name of the metric.
   * @param value to increment the metric by.
   */
  public void counter(Class<?> scope, String metricName, long value) {
    if (!counters.containsKey(metricName)) {
      MetricName name = getMetricName(scope, metricName);
      counters.put(metricName, Metrics.newCounter(name));
    }
    counters.get(metricName).inc(value);
  }

  /**
   * Creates metric name.
   * @param metricName  the name of the {@link Metric}
   * @param scope the scope of the {@link Metric}
   * @return instance of {@link MetricName}
   */
  protected MetricName getMetricName(Class<?> scope, String metricName) {
    return new MetricName(metricGroup, metricType.name(),
                                     metricName,
                                     scope == null ? null : getScope(scope));
  }

  /**
   * Sets a gauge to be measured.
   *
   * @param metricName name of the metric.
   * @param value to be associated with the metric.
   */
  public void set(String metricName, final float value) {
    if (!gauges.containsKey(metricName)) {
      MetricName name = getMetricName(null, metricName);
      gauges.put(metricName, Metrics.newGauge(name, new Gauge<Float>() {
        @Override
        public Float value() {
          return value;
        }
      }));
    }
  }

  /**
   * Measures the rate of events over time. E.g. Requests per second.
   * It also allows to track mean rate, 1,5, 15 minute moving averages.
   *
   * @param metricName name of the metric.
   * @param value to increment the meter by.
   */
  public void meter(String metricName, long value) {
    meter(null, metricName, value);
  }

  /**
   * Measures the rate of events over time for a metric within a
   * given scope of package.
   *
   * @param scope within which the metric is measured. This is essentially a tag.
   * @param metricName name of the metric.
   * @param value to increment the meter by.
   */
  public void meter(Class<?> scope, String metricName, long value) {
    if (!meters.containsKey(metricName)) {
      MetricName name = getMetricName(scope, metricName);
      meters.put(metricName,
                 Metrics.newMeter(name, metricName, TimeUnit.SECONDS));
    }
    meters.get(metricName).mark(value);
  }

  /**
   * Measures the statistical distribution of values in a stream of
   * data. It measure min, max and mean and in addition also can measure
   * median, 75th, 90th, 95th, 98th, 99th and 99.9th percentiles.
   *
   * @param metricName name of the metric.
   * @param value to be used for histogram.
   */
  public void histogram(String metricName, long value) {
    histogram(null, metricName, value);
  }

  /**
   * Measures the statistical distribution of values in a stream of
   * data. It measure min, max and mean and in addition also can measure
   * median, 75th, 90th, 95th, 98th, 99th and 99.9th percentiles.
   *
   * @param scope of the metric being measured.
   * @param metricName name of the metric.
   * @param value to be used for histogram.
   */
  public void histogram(Class<?> scope, String metricName, long value) {
    if (!histograms.containsKey(metricName)) {
      MetricName name = getMetricName(scope, metricName);
      histograms.put(metricName, Metrics.newHistogram(name));
    }
    histograms.get(metricName).update(value);
  }

  /**
   * Returns the scope based on the class specified.
   *
   * @param klass specified by class.
   * @return String representation of the scope.
   */
  private String getScope(Class<?> klass) {
    return klass.getCanonicalName();
  }

}
