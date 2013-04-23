package com.continuuity.internal.app.runtime;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.RunId;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Base class for program runtime context
 */
public abstract class ProgramRuntimeContext {
  private final String accountId;
  private final String applicationId;
  private final String programName;
  private final RunId runId;
  private final Map<String, DataSet> datasets;

  public ProgramRuntimeContext(Program program, RunId runId, Map<String, DataSet> datasets) {
    this.accountId = program.getAccountId();
    this.applicationId = program.getApplicationId();
    this.programName = program.getProgramName();
    this.runId = runId;
    this.datasets = ImmutableMap.copyOf(datasets);
  }

  public abstract Metrics getMetrics();

  @Override
  public String toString() {
    return String.format("accountId=%s, applicationId=%s, program=%s, runid=%s",
                         accountId, applicationId, programName, runId);
  }

  public <T extends DataSet> T getDataSet(String name) {
    T dataSet = (T) datasets.get(name);
    Preconditions.checkArgument(dataSet != null, "%s is not a known DataSet.", name);
    return dataSet;

  }

  public String getAccountId() {
    return accountId;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public String getProgramName() {
    return programName;
  }

  public RunId getRunId() {
    return runId;
  }

  public void injectFields(Object injectTo) {

    TypeToken<?> typeToken = TypeToken.of(injectTo.getClass());

    // Walk up the hierarchy of the class.
    for (TypeToken<?> type : typeToken.getTypes().classes()) {
      if (type.getRawType().equals(Object.class)) {
        break;
      }

      // Inject DataSet and Metrics fields.
      for (Field field : type.getRawType().getDeclaredFields()) {
        // Inject DataSet
        if (DataSet.class.isAssignableFrom(field.getType())) {
          UseDataSet dataset = field.getAnnotation(UseDataSet.class);
          if (dataset != null && !dataset.value().isEmpty()) {
            setField(injectTo, field, getDataSet(dataset.value()));
          }
          continue;
        }
        if (Metrics.class.equals(field.getType())) {
          setField(injectTo, field, getMetrics());
        }
      }
    }
  }

  private void setField(Object setTo, Field field, Object value) {
    if (!field.isAccessible()) {
      field.setAccessible(true);
    }
    try {
      field.set(setTo, value);
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
  }

}
