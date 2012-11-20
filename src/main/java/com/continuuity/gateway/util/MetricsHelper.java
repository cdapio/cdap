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
  private String metricNamePerScope;

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
    if (this.method != null) {
      Log.warn(String.format(
          "Attempt to change the method of a metrics helper in %s to %s " +
          "(old method is %s)", classe.getName(), this.method , method));
    }
    this.method = method;
    metricNamePerMethod = appendToMetric(metricNamePerConnector, method);
    this.meter(this.metricNamePerMethod, Status.Received);
  }

  public void setScope(String scope) {
    if (this.method == null) {
      Log.warn(String.format(
          "Attempt to set the scope of a metrics helper in %s to %s " +
              "but the method is not set", classe.getName(), scope));
      return;
    }
    this.scope = scope;
    metricNamePerScope = appendToMetric(metricNamePerMethod, method);
    this.meter(this.metricNamePerScope, Status.Received);
  }

  private void meter(String metric, Status status) {
    meter(metric, status, null);
  }
  private void meter(String metric, Status status, Long millis) {
    String metricName = appendToMetric(metric, status.name());
    // increment gw.connector[.method[.scope]].status
    this.metrics.meter(metricName, 1L);
    if (millis == null) return;
    // record gw.connector[.method[.scope]].status.latency
    this.metrics.histogram(appendToMetric(metricName, METRIC_LATENCY), millis);
  }

  public void finish(Status status) {
    this.meter(METRIC_NAME_BASE, status, null);
    this.meter(metricNamePerConnector, status, null);
    if (method != null) {
      long latency = System.currentTimeMillis() - startTime;
      this.meter(metricNamePerMethod, status, latency);
      if (scope != null) {
        this.meter(metricNamePerScope, status, latency);
      }
    }
  }

  public static void meterError(CMetrics metrics, String connector) {
    metrics.meter(appendToMetric(METRIC_NAME_BASE, Status.Error.name()), 1L);
    metrics.meter(appendToMetric(appendToMetric(METRIC_NAME_BASE, connector),
        Status.Error.name()), 1L);
  }
}
