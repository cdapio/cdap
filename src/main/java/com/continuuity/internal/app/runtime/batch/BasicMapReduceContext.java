package com.continuuity.internal.app.runtime.batch;

import com.continuuity.api.batch.MapReduceContext;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.app.logging.MapReduceLoggingContext;
import com.continuuity.app.metrics.MapReduceMetrics;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.internal.app.runtime.AbstractContext;
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

  private final MapReduceMetrics metrics;
  private final MapReduceLoggingContext loggingContext;

  private BatchReadable inputDataset;
  private List<Split> inputDataSelection;
  private BatchWritable outputDataset;
  private final CMetrics systemMapperMetrics;
  private final CMetrics systemReducerMetrics;
  private final TransactionAgent txAgent;
  private final Arguments runtimeArguments;

  public BasicMapReduceContext(Program program, RunId runId, Arguments runtimeArguments,
                               TransactionAgent txAgent, Map<String, DataSet> datasets,
                               MapReduceSpecification spec) {
    super(program, runId, datasets);
    this.runtimeArguments = runtimeArguments;
    this.txAgent = txAgent;
    this.systemMapperMetrics = new CMetrics(MetricType.FlowSystem, getMetricName("Mapper"));
    this.systemReducerMetrics = new CMetrics(MetricType.FlowSystem, getMetricName("Reducer"));
    this.metrics = new MapReduceMetrics(getAccountId(), getApplicationId(),
                                        getProgramName(), getRunId().toString(), getInstanceId());
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

  private String getMetricName(String task) {
    return String.format("%s.%s.%s.%s.%s.%d",
                         getAccountId(),
                         getApplicationId(),
                         getProgramName(),
                         getRunId(),
                         task,
                         getInstanceId());
  }

  @Override
  public Metrics getMetrics() {
    return metrics;
  }

  public CMetrics getSystemMapperMetrics() {
    return systemMapperMetrics;
  }

  public CMetrics getSystemReducerMetrics() {
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
