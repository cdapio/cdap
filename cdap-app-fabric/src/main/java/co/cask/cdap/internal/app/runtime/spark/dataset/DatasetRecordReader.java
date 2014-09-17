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

package co.cask.cdap.internal.app.runtime.spark.dataset;

import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetInputSplit;
import co.cask.cdap.internal.app.runtime.spark.BasicSparkContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

final class DatasetRecordReader<KEY, VALUE> extends RecordReader<KEY, VALUE> {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetRecordReader.class);
  private final SplitReader<KEY, VALUE> splitReader;
  private final BasicSparkContext context;
  private final String dataSetName;
  //TODO: Needs support for metrics when implemented

  public DatasetRecordReader(final SplitReader<KEY, VALUE> splitReader, BasicSparkContext context, String dataSetName) {
    this.splitReader = splitReader;
    this.context = context;
    this.dataSetName = dataSetName;
  }

  @Override
  public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException,
    InterruptedException {
    LoggingContextAccessor.setLoggingContext(this.context.getLoggingContext());
    DataSetInputSplit inputSplit = (DataSetInputSplit) split;
    splitReader.initialize(inputSplit.getSplit());
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    boolean hasNext = splitReader.nextKeyValue();
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
    }
  }
}
