package com.continuuity.gateway.util;

import com.continuuity.common.metrics.CMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsHelper {

  private static final Logger Log =
      LoggerFactory.getLogger(MetricsHelper.class);

  public enum Status {
    Received, Success, BadRequest, NotFound, NoData, Error
  }

  private final CMetrics metrics;

  private String method;
  private String scope;
  private Class<?> classe;
  private long startTime;

  private static final String METRIC_NAME_BASE = "gateway";
  private static final String METRIC_LATENCY = "latency";

  private String metricNamePerConnector;
  private String metricNamePerMethod;
  private String metricNamePerMethodAndScope;

  private static String appendToMetric(String base, String specific) {
    return base + "." + specific;
  }

  public MetricsHelper(Class<?> caller, CMetrics metrics,
                       String connector, String method) {
    this.classe = caller;
    this.metrics = metrics;
    this.startTime = System.currentTimeMillis();
    this.setConnector(connector);

    this.scope = null;
    this.setMethod(method);
  }

  public MetricsHelper(Class<?> caller, CMetrics metrics, String connector) {
    this(caller, metrics, connector, null);
  }

  public void setConnector(String connector) {
    this.meter(METRIC_NAME_BASE, Status.Received);
    this.metricNamePerConnector = appendToMetric(METRIC_NAME_BASE, connector);
    this.meter(this.metricNamePerConnector, Status.Received);
  }

  public void setMethod(String method) {
    if (method == null) {
      Log.warn("Attempt to set the method of a metrics helper to null in " +
          classe.getName());
      return;
    }
    if (this.method != null) {
      Log.warn(String.format(
          "Attempt to change the method of a metrics helper in %s to %s " +
          "(old method is %s)", classe.getName(), this.method , method));
    }
    // set the method and emit a "received" metric
    this.method = method;
    metricNamePerMethod = appendToMetric(metricNamePerConnector, method);
    this.meter(this.metricNamePerMethod, Status.Received);
    // if the scope is set, create and emit a combined "received" metric
    if (this.scope != null) {
      metricNamePerMethodAndScope = appendToMetric(metricNamePerMethod, scope);
      this.meter(this.metricNamePerMethodAndScope, Status.Received);
    }
  }

  public void setScope(String scope) {
    if (scope == null) {
      Log.warn("Attempt to set the scope of a metrics helper to null in " +
          classe.getName());
      return;
    }
    if (this.scope != null) {
      Log.warn(String.format(
          "Attempt to change the scope of a metrics helper in %s to %s " +
              "(old scope is %s)", classe.getName(), this.scope , scope));
    }
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
    // increment gw.connector[.method[.scope]].status
    this.metrics.meter(metricWithStatus, 1L);
    if (millis == null) return;
    // record gw.connector[.method[.scope]].latency
    this.metrics.histogram(
        appendToMetric(metric, METRIC_LATENCY), millis);
    // record gw.connector[.method[.scope]].status.latency
    this.metrics.histogram(
        appendToMetric(metricWithStatus, METRIC_LATENCY), millis);
  }

  public void finish(Status status) {
    this.meter(METRIC_NAME_BASE, status, null);
    this.meter(metricNamePerConnector, status, null);
    long latency = System.currentTimeMillis() - startTime;
    if (method != null) {
      this.meter(metricNamePerMethod, status, latency);
      if (scope != null) {
        this.meter(metricNamePerMethodAndScope, status, latency);
      }
    }
  }

  public static void meterError(CMetrics metrics, String connector) {
    metrics.meter(appendToMetric(METRIC_NAME_BASE, Status.Error.name()), 1L);
    metrics.meter(appendToMetric(appendToMetric(METRIC_NAME_BASE, connector),
        Status.Error.name()), 1L);
  }
}
