package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.api.procedure.ProcedureContext;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.metrics.ProcedureMetrics;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.RunId;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 *
 */
final class BasicProcedureContext implements ProcedureContext {

  private final String accountId;
  private final String applicationId;
  private final String procedureId;
  private final RunId runId;
  private final int instanceId;

  private final ProcedureSpecification procedureSpec;
  private final Map<String, DataSet> datasets;
  private final CMetrics systemMetrics;
  private final ProcedureMetrics procedureMetrics;

  BasicProcedureContext(Program program, int instanceId, RunId runId,
                        Map<String, DataSet> datasets, ProcedureSpecification procedureSpec) {
    this.accountId = program.getAccountId();
    this.applicationId = program.getApplicationId();
    this.procedureId = program.getProgramName();
    this.runId = runId;
    this.instanceId = instanceId;
    this.procedureSpec = procedureSpec;
    this.datasets = ImmutableMap.copyOf(datasets);
    this.systemMetrics = new CMetrics(MetricType.ProcedureSystem, getMetricName());
    this.procedureMetrics = new ProcedureMetrics(getAccountId(), getApplicationId(),
                                                 getProcedureId(), getRunId().toString(), getInstanceId());
  }

  @Override
  public <T extends DataSet> T getDataSet(String name) {
    T dataSet = (T) datasets.get(name);
    Preconditions.checkArgument(dataSet != null, "%s is not a known DataSet.", name);
    return dataSet;

  }

  @Override
  public ProcedureSpecification getSpecification() {
    return procedureSpec;
  }

  public Metrics getMetrics() {
    return procedureMetrics;
  }

  public CMetrics getSystemMetrics() {
    return systemMetrics;
  }

  public String getAccountId() {
    return accountId;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public String getProcedureId() {
    return procedureId;
  }

  public RunId getRunId() {
    return runId;
  }

  public int getInstanceId() {
    return instanceId;
  }

  private String getMetricName() {
    return String.format("%s.%s.%s.%s.%d",
                         getAccountId(),
                         getApplicationId(),
                         getProcedureId(),
                         getRunId(),
                         getInstanceId());
  }
}
