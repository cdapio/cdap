package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.api.procedure.ProcedureContext;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.logging.ProcedureLoggingContext;
import com.continuuity.app.metrics.ProcedureMetrics;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.RunId;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.internal.app.runtime.ProgramRuntimeContext;

import java.util.Map;

/**
 * Procedure runtime context
 */
final class BasicProcedureContext extends ProgramRuntimeContext implements ProcedureContext {

  private final String procedureId;
  private final int instanceId;

  private final ProcedureSpecification procedureSpec;
  private final ProcedureMetrics procedureMetrics;
  private final ProcedureLoggingContext procedureLoggingContext;

  BasicProcedureContext(Program program, RunId runId, int instanceId, Map<String, DataSet> datasets,
                        ProcedureSpecification procedureSpec) {
    super(program, runId, datasets);
    this.procedureId = program.getProgramName();
    this.instanceId = instanceId;
    this.procedureSpec = procedureSpec;
    this.procedureMetrics = new ProcedureMetrics(getAccountId(), getApplicationId(),
                                                 getProcedureId(), getRunId().toString(), getInstanceId());
    this.procedureLoggingContext = new ProcedureLoggingContext(getAccountId(), getApplicationId(), getProcedureId());
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

  public String getProcedureId() {
    return procedureId;
  }

  public int getInstanceId() {
    return instanceId;
  }

  public LoggingContext getLoggingContext() {
    return procedureLoggingContext;
  }

  @Override
  protected String getMetricName() {
    return String.format("%s.%s.%s.%s.foo.%d",
                         getAccountId(),
                         getApplicationId(),
                         getProcedureId(),
                         getRunId(),
                         getInstanceId());
  }
}
