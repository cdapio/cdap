package com.continuuity.internal.app.runtime.batch.dataset;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.batch.SplitReader;
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
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    try {
      return splitReader.nextKeyValue();
    } catch (OperationException e) {
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
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return splitReader.getProgress();
  }

  @Override
  public void close() throws IOException {
    try {
      splitReader.close();
    } finally {
      context.close();
    }
  }
}
