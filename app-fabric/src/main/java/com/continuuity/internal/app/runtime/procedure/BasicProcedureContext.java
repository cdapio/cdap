package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.api.procedure.ProcedureContext;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.logging.ProcedureLoggingContext;
import com.continuuity.app.metrics.ProcedureMetrics;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.continuuity.internal.app.runtime.AbstractContext;
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

  private final ProcedureSpecification procedureSpec;
  private final ProcedureMetrics procedureMetrics;
  private final ProcedureLoggingContext procedureLoggingContext;
  private final CMetrics systemMetrics;
  private final Arguments runtimeArguments;

  BasicProcedureContext(Program program, RunId runId, int instanceId, Map<String, DataSet> datasets,
                        Arguments runtimeArguments, ProcedureSpecification procedureSpec) {
    super(program, runId, datasets);
    this.procedureId = program.getProgramName();
    this.instanceId = instanceId;
    this.procedureSpec = procedureSpec;
    this.procedureMetrics = new ProcedureMetrics(getAccountId(), getApplicationId(),
                                                 getProcedureId(), getRunId().toString(), getInstanceId());
    this.runtimeArguments = runtimeArguments;
    this.procedureLoggingContext = new ProcedureLoggingContext(getAccountId(), getApplicationId(), getProcedureId());
    this.systemMetrics = new CMetrics(MetricType.ProcedureSystem, getMetricName());
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

  public CMetrics getSystemMetrics() {
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

  private String getMetricName() {
    return String.format("%s.%s.%s.%s.foo.%d",
                         getAccountId(),
                         getApplicationId(),
                         getProcedureId(),
                         getRunId(),
                         getInstanceId());
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
