package com.continuuity.internal.app.runtime.batch.dataset;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.internal.app.runtime.batch.BasicMapReduceContext;
import com.google.common.base.Throwables;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

class DataSetRecordWriter extends RecordWriter {
  private final BatchWritable batchWritable;
  private final BasicMapReduceContext context;

  public DataSetRecordWriter(final BatchWritable batchWritable, BasicMapReduceContext context) {
    this.batchWritable = batchWritable;
    this.context = context;
    // hack: making sure logging constext is set on the thread that accesses the runtime context
    LoggingContextAccessor.setLoggingContext(context.getLoggingContext());
  }

  @Override
  public void write(final Object key, final Object value) throws IOException {
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
