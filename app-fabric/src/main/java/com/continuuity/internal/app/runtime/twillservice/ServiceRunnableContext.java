package com.continuuity.internal.app.runtime.twillservice;

import com.continuuity.api.metrics.Metrics;
import com.continuuity.app.metrics.ServiceRunnableMetrics;
import com.continuuity.app.program.Program;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.logging.context.UserServiceLoggingContext;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillRunnableSpecification;

/**
 *
 */
final class ServiceRunnableContext {

  private final Program program;
  private final RunId runId;
  private final String serviceId;
  private final String runnableId;
  private final int instanceId;
  private final RuntimeSpecification runtimeSpecification;

  private volatile int instanceCount;
  private final ServiceRunnableMetrics serviceRunnableMetrics;

  ServiceRunnableContext(Program program, String runnableId, int instanceId, RunId runId, int instanceCount,
                         RuntimeSpecification runnableSpec, MetricsCollectionService metricsCollectionService) {
    this.program = program;
    this.runId = runId;
    this.serviceId = program.getName();
    this.runnableId = runnableId;
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
    this.runtimeSpecification = runnableSpec;
    this.serviceRunnableMetrics = new ServiceRunnableMetrics(metricsCollectionService, program.getApplicationId(),
                                                             serviceId, runnableId);

  }

  public String toString() {
    return String.format("runnable=%s, instance=%d, accountId=%s, applicationId=%s, program=%s, runid=%s",
                         getRunnableId(), getInstanceId(), getInstanceCount(), getApplicationId(), getServiceId(),
                         runId);
  }

  public String getAccountId() {
    return program.getAccountId();
  }

  public String getRunnableId() {
    return runnableId;
  }

  public int getInstanceId() {
    return instanceId;
  }

  public int getInstanceCount() {
    return instanceCount;
  }

  public String getApplicationId() {
    return program.getApplicationId();
  }

  public String getServiceId() {
    return program.getName();
  }

  public void setInstanceCount(int count) {
    instanceCount = count;
  }

  public TwillRunnableSpecification getSpecification() {
    return runtimeSpecification.getRunnableSpecification();
  }

  public Metrics getMetrics() {
    return serviceRunnableMetrics;
  }

  public RunId getRunId() {
    return runId;
  }

  public String getMetricContext() {
    return String.format("%s.s.%s.%s.%d", getApplicationId(), getServiceId(), getRunnableId(), getInstanceId());
  }

  public LoggingContext getLoggingContext() {
    return new UserServiceLoggingContext(getAccountId(), getApplicationId(), getServiceId(), getRunnableId());
  }

}
