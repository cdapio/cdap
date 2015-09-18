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

import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.internal.app.runtime.batch.BasicMapReduceTaskContext;
import com.google.common.base.Throwables;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

final class DataSetRecordWriter<KEY, VALUE> extends RecordWriter<KEY, VALUE> {
  private final BatchWritable<KEY, VALUE> batchWritable;
  private final BasicMapReduceTaskContext context;

  public DataSetRecordWriter(BatchWritable<KEY, VALUE> batchWritable, BasicMapReduceTaskContext context) {
    this.batchWritable = batchWritable;
    this.context = context;

    // hack: making sure logging context is set on the thread that accesses the runtime context
    LoggingContextAccessor.setLoggingContext(this.context.getLoggingContext());
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
      this.context.flushOperations();
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class, InterruptedException.class);
      throw Throwables.propagate(e);
    }
  }
}
