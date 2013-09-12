package com.continuuity.internal.app.runtime;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.app.program.Program;
import com.continuuity.weave.api.RunId;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Base class for program runtime context
 * TODO: ENG-2702 opened to fix the deprecated AbstractContext and cleanup related to context overall.
 */
@Deprecated
public abstract class AbstractContext {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractContext.class);

  private final Program program;
  private final RunId runId;
  private final Map<String, DataSet> datasets;

  public AbstractContext(Program program, RunId runId, Map<String, DataSet> datasets) {
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

  public <T extends DataSet> T getDataSet(String name) {
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

  protected final MetricsCollector getMetricsCollector(MetricsScope scope,
                                                       MetricsCollectionService collectionService, String context) {
    // NOTE: RunId metric is not supported now. Need UI refactoring to enable it.
    return collectionService.getCollector(scope, context, "0");
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

  /**
   * Release all resources held by this context, for example, datasets. Subclasses should override this
   * method to release additional resources.
   */
  public void close() {
    for (DataSet ds : datasets.values()) {
      closeDataSet(ds);
    }
  }

  /**
   * Closes one dataset; logs but otherwise ignores exceptions.
   */
  protected void closeDataSet(DataSet ds) {
    try {
      ds.close();
    } catch (Throwable t) {
      LOG.error("Dataset throws exceptions during close:" + ds.getName() + ", in context: " + this);
    }
  }
}
