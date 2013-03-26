package com.continuuity.internal.app.runtime.batch.hadoop;

import com.continuuity.api.batch.hadoop.MapReduceContext;
import com.continuuity.api.batch.hadoop.MapReduceSpecification;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.app.runtime.RunId;
import com.continuuity.internal.app.runtime.batch.BasicBatchContext;
import org.apache.hadoop.mapreduce.Job;

import java.util.List;

/**
 *
 */
public class BasicMapReduceContext implements MapReduceContext {
  private final MapReduceSpecification spec;
  private final BasicBatchContext batchContext;
  private final Job job;
  private final RunId runId;

  private BatchReadable inputDataset;
  private List<Split> inputDataSelection;
  private BatchWritable outputDataset;

  public BasicMapReduceContext(MapReduceSpecification spec, BasicBatchContext batchContext, Job job, RunId runId) {
    this.spec = spec;
    this.batchContext = batchContext;
    this.job = job;
    this.runId = runId;
  }

  @Override
  public String toString() {
    return String.format("job=%s, runid=%s",
                         spec.getName(), runId);
  }

  @Override
  public <T extends DataSet> T getDataSet(String name) {
    return batchContext.getDataSet(name);
  }

  @Override
  public MapReduceSpecification getSpecification() {
    return spec;
  }

  @Override
  public <T> T getHadoopJobConf() {
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

  public BatchReadable getInputDataset() {
    return inputDataset;
  }

  public List<Split> getInputDataSelection() {
    return inputDataSelection;
  }

  public BatchWritable getOutputDataset() {
    return outputDataset;
  }
}
