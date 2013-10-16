package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.api.procedure.ProcedureContext;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.metrics.ProcedureMetrics;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.internal.app.runtime.AbstractContext;
import com.continuuity.logging.context.ProcedureLoggingContext;
import com.continuuity.weave.api.RunId;
import com.google.common.collect.ImmutableMap;

import java.util.Iterator;
import java.util.Map;

/**
 * Procedure runtime context
 */
final class BasicProcedureContext extends AbstractContext implements ProcedureContext {

  private final String procedureId;
  private final int instanceId;
  private volatile int instanceCount;

  private final ProcedureSpecification procedureSpec;
  private final ProcedureMetrics procedureMetrics;
  private final ProcedureLoggingContext procedureLoggingContext;
  private final MetricsCollector systemMetrics;
  private final Arguments runtimeArguments;

  BasicProcedureContext(Program program, RunId runId, int instanceId, int instanceCount, Map<String, DataSet> datasets,
                        Arguments runtimeArguments, ProcedureSpecification procedureSpec,
                        MetricsCollectionService collectionService) {
    super(program, runId, datasets);
    this.procedureId = program.getName();
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
    this.procedureSpec = procedureSpec;
    this.procedureMetrics = new ProcedureMetrics(collectionService, getApplicationId(), getProcedureId());
    this.runtimeArguments = runtimeArguments;
    this.procedureLoggingContext = new ProcedureLoggingContext(getAccountId(), getApplicationId(), getProcedureId());
    this.systemMetrics = getMetricsCollector(MetricsScope.REACTOR, collectionService, getMetricsContext());
  }

  @Override
  public String toString() {
    return String.format("procedure=%s, instance=%d, %s", getProcedureId(), getInstanceId(), super.toString());
  }

  @Override
  public ProcedureSpecification getSpecification() {
    return procedureSpec;
  }

  @Override
  public Metrics getMetrics() {
    return procedureMetrics;
  }

  @Override
  public int getInstanceCount() {
    return instanceCount;
  }

  public MetricsCollector getSystemMetrics() {
    return systemMetrics;
  }

  public String getProcedureId() {
    return procedureId;
  }

  public int getInstanceId() {
    return instanceId;
  }

  public LoggingContext getLoggingContext() {
    return procedureLoggingContext;
  }

  private String getMetricsContext() {
    return String.format("%s.p.%s.%d", getApplicationId(), getProcedureId(), getInstanceId());
  }

  public void setInstanceCount(int count) {
    instanceCount = count;
  }

  /**
   * @return A map of argument key and value.
   */
  @Override
  public Map<String, String> getRuntimeArguments() {
    ImmutableMap.Builder<String, String> arguments = ImmutableMap.builder();
    Iterator<Map.Entry<String, String>> it = runtimeArguments.iterator();
    while (it.hasNext()) {
      arguments.put(it.next());
    }
    return arguments.build();
  }
}
