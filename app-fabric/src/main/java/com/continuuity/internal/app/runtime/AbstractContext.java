package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.app.program.Program;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;

/**
 * Base class for program runtime context
 * TODO: ENG-2702 opened to fix the deprecated AbstractContext and cleanup related to context overall.
 */
@Deprecated
public abstract class AbstractContext implements DataSetContext {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractContext.class);

  private final Program program;
  private final RunId runId;
  private final Map<String, Closeable> datasets;

  public AbstractContext(Program program, RunId runId, Map<String, Closeable> datasets) {
    this.program = program;
    this.runId = runId;
    this.datasets = ImmutableMap.copyOf(datasets);
  }

  public abstract Metrics getMetrics();

  @Override
  public String toString() {
    return String.format("accountId=%s, applicationId=%s, program=%s, runid=%s",
                         getAccountId(), getApplicationId(), getProgramName(), runId);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Closeable> T getDataSet(String name) {
    T dataSet = (T) datasets.get(name);
    Preconditions.checkArgument(dataSet != null, "%s is not a known DataSet.", name);
    return dataSet;
  }

  public String getAccountId() {
    return program.getAccountId();
  }

  public String getApplicationId() {
    return program.getApplicationId();
  }

  public String getProgramName() {
    return program.getName();
  }

  public Program getProgram() {
    return program;
  }

  public RunId getRunId() {
    return runId;
  }

  protected final MetricsCollector getMetricsCollector(MetricsScope scope,
                                                       MetricsCollectionService collectionService, String context) {
    // NOTE: RunId metric is not supported now. Need UI refactoring to enable it.
    return collectionService.getCollector(scope, context, "0");
  }

  /**
   * Release all resources held by this context, for example, datasets. Subclasses should override this
   * method to release additional resources.
   */
  public void close() {
    for (Closeable ds : datasets.values()) {
      closeDataSet(ds);
    }
  }

  /**
   * Closes one dataset; logs but otherwise ignores exceptions.
   */
  protected void closeDataSet(Closeable ds) {
    try {
      ds.close();
    } catch (Throwable t) {
      LOG.error("Dataset throws exceptions during close:" + ds.toString() + ", in context: " + this);
    }
  }
}
