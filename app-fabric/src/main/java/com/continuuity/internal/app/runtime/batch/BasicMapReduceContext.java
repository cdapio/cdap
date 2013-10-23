package com.continuuity.internal.app.runtime.batch;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.app.metrics.MapReduceMetrics;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.internal.app.runtime.AbstractContext;
import com.continuuity.logging.context.MapReduceLoggingContext;
import com.continuuity.weave.api.RunId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.mapreduce.Job;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Mapreduce job runtime context
 */
public class BasicMapReduceContext extends AbstractContext implements MapReduceContext {

  private final MapReduceSpecification spec;
  private final MapReduceLoggingContext loggingContext;
  private final Map<MetricsScope, MetricsCollector> systemMapperMetrics;
  private final Map<MetricsScope, MetricsCollector> systemReducerMetrics;
  private final Arguments runtimeArguments;
  private final long logicalStartTime;
  private final String workflowBatch;
  private final Metrics mapredMetrics;
  private final MetricsCollectionService metricsCollectionService;

  private BatchReadable inputDataset;
  private List<Split> inputDataSelection;
  private BatchWritable outputDataset;
  private Job job;

  // todo: having it here seems like a hack will be fixed with further post-integration refactoring
  private final Iterable<TransactionAware> txAwares;

  public BasicMapReduceContext(Program program,
                               MapReduceMetrics.TaskType type,
                               RunId runId,
                               Arguments runtimeArguments,
                               Map<String, DataSet> datasets,
                               MapReduceSpecification spec,
                               Iterable<TransactionAware> txAwares,
                               long logicalStartTime,
                               String workflowBatch) {
    this(program, type, runId, runtimeArguments, datasets, spec, txAwares, logicalStartTime, workflowBatch, null);
  }


  public BasicMapReduceContext(Program program,
                               MapReduceMetrics.TaskType type,
                               RunId runId,
                               Arguments runtimeArguments,
                               Map<String, DataSet> datasets,
                               MapReduceSpecification spec,
                               Iterable<TransactionAware> txAwares,
                               long logicalStartTime,
                               String workflowBatch,
                               MetricsCollectionService metricsCollectionService) {
    super(program, runId, datasets);
    this.runtimeArguments = runtimeArguments;
    this.logicalStartTime = logicalStartTime;
    this.workflowBatch = workflowBatch;
    this.metricsCollectionService = metricsCollectionService;

    if (metricsCollectionService != null) {
      this.systemMapperMetrics = Maps.newHashMap();
      this.systemReducerMetrics = Maps.newHashMap();
      for (MetricsScope scope : MetricsScope.values()) {
        this.systemMapperMetrics.put(scope, getMetricsCollector(scope, metricsCollectionService,
                                                                getMetricContext(MapReduceMetrics.TaskType.Mapper)));
        this.systemReducerMetrics.put(scope, getMetricsCollector(scope, metricsCollectionService,
                                                                 getMetricContext(MapReduceMetrics.TaskType.Reducer)));
      }
      // for user metrics.  type can be null if its not in a map or reduce task, but in the yarn container that
      // launches the mapred job.
      this.mapredMetrics = (type == null) ?
        null : new MapReduceMetrics(metricsCollectionService, getApplicationId(), getProgramName(), type);
    } else {
      this.systemMapperMetrics = null;
      this.systemReducerMetrics = null;
      this.mapredMetrics = null;
    }
    this.loggingContext = new MapReduceLoggingContext(getAccountId(), getApplicationId(), getProgramName());
    this.spec = spec;
    this.txAwares = txAwares;
  }

  @Override
  public String toString() {
    return String.format("job=%s,=%s",
                         spec.getName(), super.toString());
  }

  @Override
  public MapReduceSpecification getSpecification() {
    return spec;
  }

  @Override
  public long getLogicalStartTime() {
    return logicalStartTime;
  }

  /**
   * Returns the name of the Batch job when running inside workflow. Otherwise, return null.
   */
  public String getWorkflowBatch() {
    return workflowBatch;
  }

  public void setJob(Job job) {
    this.job = job;
  }

  @Override
  public <T> T getHadoopJob() {
    return (T) job;
  }

  @Override
  public void setInput(BatchReadable dataset, List<Split> splits) {
    this.inputDataset = dataset;
    this.inputDataSelection = splits;
  }

  @Override
  public void setOutput(BatchWritable dataset) {
    this.outputDataset = dataset;
  }

  public int getInstanceId() {
    return 0;
  }

  private String getMetricContext(MapReduceMetrics.TaskType type) {
    return String.format("%s.b.%s.%s.%d",
                         getApplicationId(),
                         getProgramName(),
                         type.getId(),
                         getInstanceId());
  }

  @Override
  public Metrics getMetrics() {
    return mapredMetrics;
  }

  public MetricsCollectionService getMetricsCollectionService() {
    return metricsCollectionService;
  }

  public MetricsCollector getSystemMapperMetrics() {
    return systemMapperMetrics.get(MetricsScope.REACTOR);
  }

  public MetricsCollector getSystemReducerMetrics() {
    return systemReducerMetrics.get(MetricsScope.REACTOR);
  }

  public MetricsCollector getSystemMapperMetrics(MetricsScope scope) {
    return systemMapperMetrics.get(scope);
  }

  public MetricsCollector getSystemReducerMetrics(MetricsScope scope) {
    return systemReducerMetrics.get(scope);
  }

  public LoggingContext getLoggingContext() {
    return loggingContext;
  }

  public BatchReadable getInputDataset() {
    return inputDataset;
  }

  public List<Split> getInputDataSelection() {
    return inputDataSelection;
  }

  public BatchWritable getOutputDataset() {
    return outputDataset;
  }

  Arguments getRuntimeArgs() {
    return runtimeArguments;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    ImmutableMap.Builder<String, String> arguments = ImmutableMap.builder();
    Iterator<Map.Entry<String, String>> it = runtimeArguments.iterator();
    while (it.hasNext()) {
      arguments.put(it.next());
    }
    return arguments.build();
  }

  public void flushOperations() throws Exception {
    for (TransactionAware txAware : txAwares) {
      txAware.commitTx();
    }
  }
}
