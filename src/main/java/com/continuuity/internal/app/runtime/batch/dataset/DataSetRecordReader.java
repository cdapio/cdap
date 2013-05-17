package com.continuuity.internal.app.runtime.batch.dataset;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.internal.app.runtime.batch.BasicMapReduceContext;
import com.google.common.base.Throwables;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

final class DataSetRecordReader<KEY, VALUE> extends RecordReader<KEY, VALUE> {
  private final SplitReader<KEY, VALUE> splitReader;
  private final BasicMapReduceContext context;

  public DataSetRecordReader(final SplitReader<KEY, VALUE> splitReader,
                             BasicMapReduceContext context) {
    this.splitReader = splitReader;
    this.context = context;
  }

  @Override
  public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException,
                                                                                          InterruptedException {
    // hack: making sure logging constext is set on the thread that accesses the runtime context
    LoggingContextAccessor.setLoggingContext(this.context.getLoggingContext());

    DataSetInputSplit inputSplit = (DataSetInputSplit) split;

    try {
      splitReader.initialize(inputSplit.getSplit());
    } catch(OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    try {
      boolean success = splitReader.nextKeyValue();
      // This and below metrics collection code is "hacky": it just tracks smth for the fake UI we have for mr jobs now
      context.getSystemMapperMetrics().meter(DataSetRecordReader.class, "tuples.attempt.read", 1);
      if (success) {
        // "Input" is the name of the fake stream for UI which mimics the input source of data for the mapreduce job
        context.getSystemMapperMetrics().counter("Input" +
                                                   FlowletDefinition.INPUT_ENDPOINT_POSTFIX + ".stream.in", 1);
        context.getSystemMapperMetrics().meter(DataSetRecordReader.class, "tuples.read", 1);
      }
      return success;
    } catch(OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public KEY getCurrentKey() throws IOException, InterruptedException {
    return splitReader.getCurrentKey();
  }

  @Override
  public VALUE getCurrentValue() throws IOException, InterruptedException {
    try {
      return splitReader.getCurrentValue();
    } catch(OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return splitReader.getProgress();
  }

  @Override
  public void close() throws IOException {
    splitReader.close();
  }
}
