package com.continuuity.internal.app.runtime.batch.dataset;

import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.internal.app.runtime.batch.BasicMapReduceContext;
import com.google.common.base.Throwables;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

final class DataSetRecordWriter<KEY, VALUE> extends RecordWriter<KEY, VALUE> {
  private static final Logger LOG = LoggerFactory.getLogger(DataSetRecordWriter.class);

  private final BatchWritable<KEY, VALUE> batchWritable;
  private final BasicMapReduceContext mrContext;

  public DataSetRecordWriter(final BatchWritable<KEY, VALUE> batchWritable, BasicMapReduceContext mrContext) {
    this.batchWritable = batchWritable;
    this.mrContext = mrContext;
    // hack: making sure logging context is set on the thread that accesses the runtime context
    LoggingContextAccessor.setLoggingContext(mrContext.getLoggingContext());
  }

  @Override
  public void write(final KEY key, final VALUE value) throws IOException {
    batchWritable.write(key, value);
  }

  @Override
  public void close(final TaskAttemptContext context) throws IOException, InterruptedException {
    // transaction is not finished, but we want all operations to be dispatched (some could be buffered in memory by tx
    // agent)
    try {
      mrContext.flushOperations();
    } catch (Exception e) {
      LOG.error("Failed to flush operations at the end of reducer of " + mrContext.toString());
      throw Throwables.propagate(e);
    } finally {
      mrContext.close();
      // sleep to allow metrics to be emitted
      TimeUnit.SECONDS.sleep(2L);
      mrContext.getMetricsCollectionService().stop();
    }
  }
}
