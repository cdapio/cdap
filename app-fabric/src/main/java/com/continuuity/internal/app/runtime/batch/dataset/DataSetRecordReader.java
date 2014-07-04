package com.continuuity.internal.app.runtime.batch.dataset;

import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.internal.app.runtime.batch.BasicMapReduceContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

final class DataSetRecordReader<KEY, VALUE> extends RecordReader<KEY, VALUE> {
  private static final Logger LOG = LoggerFactory.getLogger(DataSetRecordReader.class);
  private final SplitReader<KEY, VALUE> splitReader;
  private final BasicMapReduceContext context;
  private final String dataSetName;
  private final MetricsCollector dataSetMetrics;

  public DataSetRecordReader(final SplitReader<KEY, VALUE> splitReader,
                             BasicMapReduceContext context, String dataSetName) {
    this.splitReader = splitReader;
    this.context = context;
    this.dataSetName = dataSetName;
    this.dataSetMetrics = context.getMetricsCollectionService().getCollector(
      MetricsScope.REACTOR, Constants.Metrics.DATASET_CONTEXT, "0");
  }

  @Override
  public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException,
                                                                                          InterruptedException {
    // hack: making sure logging context is set on the thread that accesses the runtime context
    LoggingContextAccessor.setLoggingContext(this.context.getLoggingContext());
    DataSetInputSplit inputSplit = (DataSetInputSplit) split;
    splitReader.initialize(inputSplit.getSplit());
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    boolean hasNext = splitReader.nextKeyValue();
    if (hasNext) {
      // splitreader doesn't increment these metrics, need to do it ourselves.
      context.getSystemMapperMetrics().gauge("store.reads", 1, dataSetName);
      context.getSystemMapperMetrics().gauge("store.ops", 1, dataSetName);
      dataSetMetrics.gauge("dataset.store.reads", 1, dataSetName);
      dataSetMetrics.gauge("dataset.store.ops", 1, dataSetName);
    }
    return hasNext;
  }

  @Override
  public KEY getCurrentKey() throws IOException, InterruptedException {
    return splitReader.getCurrentKey();
  }

  @Override
  public VALUE getCurrentValue() throws IOException, InterruptedException {
    return splitReader.getCurrentValue();
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
      // sleep to allow metrics to be emitted
      try {
        TimeUnit.SECONDS.sleep(2L);
      } catch (InterruptedException e) {
        LOG.info("sleep interrupted while waiting for final metrics to be emitted", e);
      } finally {
        context.getMetricsCollectionService().stop();
      }
    }
  }
}
