package com.continuuity.internal.app.runtime.batch;

import com.continuuity.api.batch.MapReduceContext;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.app.metrics.MapReduceMetrics;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.internal.app.runtime.AbstractContext;
import com.continuuity.logging.context.MapReduceLoggingContext;
import com.continuuity.weave.api.RunId;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.mapreduce.Job;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Mapreduce job runtime context
 */
public class BasicMapReduceContext extends AbstractContext implements MapReduceContext {
  private final MapReduceSpecification spec;
  private Job job;

  private final MapReduceLoggingContext loggingContext;

  private BatchReadable inputDataset;
  private List<Split> inputDataSelection;
  private BatchWritable outputDataset;
  private final MetricsCollector systemMapperMetrics;
  private final MetricsCollector systemReducerMetrics;
  private final TransactionAgent txAgent;
  private final Arguments runtimeArguments;

  public BasicMapReduceContext(Program program, RunId runId, Arguments runtimeArguments,
                               TransactionAgent txAgent, Map<String, DataSet> datasets,
                               MapReduceSpecification spec) {
    this(program, runId, runtimeArguments, txAgent, datasets, spec, null);
  }


  public BasicMapReduceContext(Program program, RunId runId, Arguments runtimeArguments,
                               TransactionAgent txAgent, Map<String, DataSet> datasets,
                               MapReduceSpecification spec, MetricsCollectionService metricsCollectionService) {
    super(program, runId, datasets);
    this.runtimeArguments = runtimeArguments;
    this.txAgent = txAgent;

    if (metricsCollectionService != null) {
      this.systemMapperMetrics = getMetricsCollector(MetricsScope.REACTOR, metricsCollectionService,
                                                     getMetricContext(MapReduceMetrics.TaskType.Mapper));
      this.systemReducerMetrics = getMetricsCollector(MetricsScope.REACTOR, metricsCollectionService,
                                                      getMetricContext(MapReduceMetrics.TaskType.Reducer));
    } else {
      this.systemMapperMetrics = null;
      this.systemReducerMetrics = null;
    }
    this.loggingContext = new MapReduceLoggingContext(getAccountId(), getApplicationId(), getProgramName());
    this.spec = spec;
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
    throw new UnsupportedOperationException("User metrics should be emitted through the MR framework.");
  }

  public MetricsCollector getSystemMapperMetrics() {
    return systemMapperMetrics;
  }

  public MetricsCollector getSystemReducerMetrics() {
    return systemReducerMetrics;
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

  public void flushOperations() throws OperationException {
    txAgent.flush();
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

}
