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
import co.cask.cdap.internal.app.runtime.batch.BasicMapReduceTaskContext;
import co.cask.cdap.internal.app.runtime.batch.MapReduceClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * An {@link OutputFormat} for writing into dataset.
 * @param <KEY> Type of key.
 * @param <VALUE> Type of value.
 */
public final class DataSetOutputFormat<KEY, VALUE> extends OutputFormat<KEY, VALUE> {
  public static final String HCONF_ATTR_OUTPUT_DATASET = "output.dataset.name";

  @Override
  public RecordWriter<KEY, VALUE> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    MapReduceClassLoader classLoader = MapReduceClassLoader.getFromConfiguration(conf);
    BasicMapReduceTaskContext taskContext = classLoader.getTaskContextProvider().get(context);
    BatchWritable<KEY, VALUE> dataset = taskContext.getDataset(getOutputDataSet(conf));
    return new DataSetRecordWriter<>(dataset, taskContext);
  }

  private String getOutputDataSet(Configuration conf) {
    return conf.get(HCONF_ATTR_OUTPUT_DATASET);
  }

  @Override
  public void checkOutputSpecs(final JobContext context) throws IOException, InterruptedException {
    if (getOutputDataSet(context.getConfiguration()) == null) {
      throw new IllegalArgumentException("Dataset name not configured for job: " + context.getJobName());
    }
    // TODO: validate out types? Or this is ensured by configuring job in "internal" code (i.e. not in user code)
  }

  @Override
  public OutputCommitter getOutputCommitter(final TaskAttemptContext context) throws IOException, InterruptedException {
    return new DataSetOutputCommitter();
  }
}
