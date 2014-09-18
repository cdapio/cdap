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

import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.internal.app.runtime.spark.BasicSparkContext;
import com.google.common.base.Throwables;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

final class DatasetRecordWriter<KEY, VALUE> extends RecordWriter<KEY, VALUE> {
  //TODO: Needs support for metrics
  private static final Logger LOG = LoggerFactory.getLogger(DatasetRecordWriter.class);

  private final BatchWritable<KEY, VALUE> batchWritable;
  private final BasicSparkContext sparkContext;

  public DatasetRecordWriter(final BatchWritable<KEY, VALUE> batchWritable, BasicSparkContext sparkContext) {
    this.batchWritable = batchWritable;
    this.sparkContext = sparkContext;
    LoggingContextAccessor.setLoggingContext(sparkContext.getLoggingContext());
  }

  @Override
  public void write(final KEY key, final VALUE value) throws IOException {
    batchWritable.write(key, value);
  }

  @Override
  public void close(final TaskAttemptContext context) throws IOException, InterruptedException {
    try {
      sparkContext.flushOperations();
    } catch (Exception e) {
      LOG.error("Failed to flush operations at the end of reducer of " + sparkContext.toString());
      throw Throwables.propagate(e);
    } finally {
      sparkContext.close();
    }
  }
}
