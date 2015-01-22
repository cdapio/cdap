/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.batch.dataset;

import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.internal.app.runtime.batch.BasicMapReduceContext;
import com.google.common.collect.ImmutableMap;
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
  private final MetricsCollector dataSetMetrics;

  public DataSetRecordReader(final SplitReader<KEY, VALUE> splitReader,
                             BasicMapReduceContext context, String dataSetName) {
    this.splitReader = splitReader;
    this.context = context;
    this.dataSetMetrics = context.getMetricsCollectionService().getCollector(
      ImmutableMap.of(Constants.Metrics.Tag.DATASET, dataSetName,
                      Constants.Metrics.Tag.RUN_ID, context.getRunId().getId()));
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
      context.getProgramMetrics().increment("store.reads", 1);
      context.getProgramMetrics().increment("store.ops", 1);
      dataSetMetrics.increment("dataset.store.reads", 1);
      dataSetMetrics.increment("dataset.store.ops", 1);
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
