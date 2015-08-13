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
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.internal.app.runtime.batch.BasicMapReduceContext;
import co.cask.cdap.internal.app.runtime.batch.MapReduceContextProvider;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

final class DataSetRecordReader<KEY, VALUE> extends RecordReader<KEY, VALUE> {
  private static final Logger LOG = LoggerFactory.getLogger(DataSetRecordReader.class);
  private final SplitReader<KEY, VALUE> splitReader;
  private final MapReduceContextProvider mrContextProvider;
  private final BasicMapReduceContext context;

  public DataSetRecordReader(final SplitReader<KEY, VALUE> splitReader,
                             final MapReduceContextProvider mrContextProvider) {
    this.splitReader = splitReader;
    this.mrContextProvider = mrContextProvider;
    this.context = mrContextProvider.get();
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
    return splitReader.nextKeyValue();
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
      try {
        context.close();
      } finally {
        mrContextProvider.stop();
      }
    }
  }
}
