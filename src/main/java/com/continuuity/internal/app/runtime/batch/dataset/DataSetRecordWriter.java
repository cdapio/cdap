package com.continuuity.internal.app.runtime.batch.dataset;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.internal.app.runtime.batch.BasicMapReduceContext;
import com.google.common.base.Throwables;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

final class DataSetRecordWriter<KEY, VALUE> extends RecordWriter<KEY, VALUE> {
  private final BatchWritable<KEY, VALUE> batchWritable;

  public DataSetRecordWriter(final BatchWritable<KEY, VALUE> batchWritable, BasicMapReduceContext context) {
    this.batchWritable = batchWritable;
    // hack: making sure logging constext is set on the thread that accesses the runtime context
    LoggingContextAccessor.setLoggingContext(context.getLoggingContext());
  }

  @Override
  public void write(final KEY key, final VALUE value) throws IOException {
    try {
      batchWritable.write(key, value);
    } catch(OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close(final TaskAttemptContext context) throws IOException, InterruptedException {
    // DO NOTHING
  }
}
