package com.continuuity.common.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/**
 * Helper class for metrics.
 */
public class MetricsHelper {

  private static final Logger LOG =
      LoggerFactory.getLogger(MetricsHelper.class);

  /**
   * Status returned from metrics collection service.
   */
  public enum Status {
    Received, Success, BadRequest, NotFound, NoData, Error
  }

  private final CMetrics metrics;

  private String method;
  private String scope;
  private Class<?> classe;
  private long startTime;

  private static final String METRIC_LATENCY = "latency";

  private String metricNamePerQualifier;
  private String metricNamePerMethod;
  private String metricNamePerMethodAndScope;

  private static String appendToMetric(String base, String specific) {
    return base + "." + specific;
  }

  public MetricsHelper(MetricsHelper other) {
    metrics = other.metrics;

    method = other.method;
    scope = other.scope;
    classe = other.classe;
    startTime = other.startTime;

    metricNamePerQualifier = other.metricNamePerQualifier;
    metricNamePerMethod = other.metricNamePerMethod;
    metricNamePerMethodAndScope = other.metricNamePerMethodAndScope;
  }

  public MetricsHelper(Class<?> caller, CMetrics metrics,
                       String qualifier, String method) {
    this.classe = caller;
    this.metrics = metrics;
    this.startTime = System.currentTimeMillis();
    this.setQualifier(qualifier);

    this.scope = null;
    if (method != null) {
      this.setMethod(method);
    }
  }

  public MetricsHelper(Class<?> caller, CMetrics metrics, String qualifier) {
    this(caller, metrics, qualifier, null);
  }

  public void setQualifier(String qualifier) {
    this.metricNamePerQualifier = qualifier;
    this.meter(this.metricNamePerQualifier, Status.Received);
  }

  public void setMethod(String method) {
    if (method == null) {
      LOG.warn("Attempt to set the method of a metrics helper to null in " +
          classe.getName());
      return;
    }
    if (this.method != null) {
      LOG.warn(String.format(
          "Attempt to change the method of a metrics helper in %s to %s " +
              "(old method is %s)", classe.getName(), this.method, method));
    }
    // set the method and emit a "received" metric
    this.method = method;
    metricNamePerMethod = appendToMetric(metricNamePerQualifier, method);
    this.meter(this.metricNamePerMethod, Status.Received);
    // if the scope is set, create and emit a combined "received" metric
    if (this.scope != null) {
      metricNamePerMethodAndScope = appendToMetric(metricNamePerMethod, scope);
      this.meter(this.metricNamePerMethodAndScope, Status.Received);
    }
  }

  public void setScope(byte[] scope) {
    setScope(new String(scope));
  }

  static final Pattern PATTERN = Pattern.compile("[:/]+");

  public void setScope(String scope) {
    if (scope == null) {
      LOG.warn("Attempt to set the scope of a metrics helper to null in " +
          classe.getName());
      return;
    }
    if (this.scope != null) {
      LOG.warn(String.format(
          "Attempt to change the scope of a metrics helper in %s to %s " +
              "(old scope is %s)", classe.getName(), this.scope, scope));
    }
    // if (scope.contains(":"))
    scope = PATTERN.matcher(scope).replaceAll(".");

    // set the scope
    this.scope = scope;
    // if the method is set, create and emit a combined "received" metric
    if (this.method != null) {
      metricNamePerMethodAndScope = appendToMetric(metricNamePerMethod, scope);
      this.meter(this.metricNamePerMethodAndScope, Status.Received);
    }
  }

  private void meter(String metric, Status status) {
    meter(metric, status, null);
  }
  private void meter(String metric, Status status, Long millis) {
    String metricWithStatus = appendToMetric(metric, status.name());
    // increment qualifier[.method[.scope]].status
    count(metricWithStatus, 1L);
    if (millis == null) {
      return;
    }
    // record qualifier[.method[.scope]].latency
    this.metrics.histogram(
        appendToMetric(metric, METRIC_LATENCY), millis);
    // record qualifier[.method[.scope]].status.latency
    this.metrics.histogram(
        appendToMetric(metricWithStatus, METRIC_LATENCY), millis);
  }

  protected void count(String metricWithStatus, long count) {
    this.metrics.meter(metricWithStatus, count);
  }

  public void finish(Status status) {
    this.meter(metricNamePerQualifier, status, null);
    long latency = System.currentTimeMillis() - startTime;
    if (method != null) {
      this.meter(metricNamePerMethod, status, latency);
      if (scope != null) {
        this.meter(metricNamePerMethodAndScope, status, latency);
      }
    }
  }

  public void success() {
    finish(Status.Success);
  }

  public void failure() {
    finish(Status.Error);
  }

  public static void meterError(CMetrics metrics, String qualifier) {
    metrics.meter(appendToMetric(qualifier, Status.Error.name()), 1L);
  }
}
