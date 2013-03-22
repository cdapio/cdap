package com.continuuity.internal.app.runtime.batch.hadoop.dataset;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.batch.BatchWritable;
import com.google.common.base.Throwables;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

class DataSetRecordWriter extends RecordWriter {
  private BatchWritable batchWritable;

  public DataSetRecordWriter(final BatchWritable batchWritable) {
    this.batchWritable = batchWritable;
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
