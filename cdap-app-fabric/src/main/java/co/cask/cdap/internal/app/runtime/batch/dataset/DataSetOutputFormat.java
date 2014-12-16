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
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.internal.app.runtime.batch.BasicMapReduceContext;
import co.cask.cdap.internal.app.runtime.batch.MapReduceContextProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * An {@link OutputFormat} for writing into dataset.
 * @param <KEY> Type of key.
 * @param <VALUE> Type of value.
 */
public final class DataSetOutputFormat<KEY, VALUE> extends OutputFormat<KEY, VALUE> {
  private static final Logger LOG = LoggerFactory.getLogger(DataSetOutputFormat.class);
  public static final String HCONF_ATTR_OUTPUT_DATASET = "output.dataset.name";

  public static void setOutput(Job job, String outputDatasetName) {
    job.setOutputFormatClass(DataSetOutputFormat.class);
    job.getConfiguration().set(HCONF_ATTR_OUTPUT_DATASET, outputDatasetName);
  }

  @Override
  public RecordWriter<KEY, VALUE> getRecordWriter(final TaskAttemptContext context)
    throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    // we don't currently allow datasets as the format between map and reduce stages, otherwise we'll have to
    // pass in the stage here instead of hardcoding reducer.
    MapReduceContextProvider contextProvider = new MapReduceContextProvider(context, MapReduceMetrics.TaskType.Reducer);
    BasicMapReduceContext mrContext = contextProvider.get();
    mrContext.getMetricsCollectionService().startAndWait();
    @SuppressWarnings("unchecked")
    BatchWritable<KEY, VALUE> dataset = (BatchWritable<KEY, VALUE>) mrContext.getDataset(getOutputDataSet(conf));

    // the record writer now owns the context and will close it
    return new DataSetRecordWriter<KEY, VALUE>(dataset, mrContext);
  }

  private String getOutputDataSet(Configuration conf) {
    return conf.get(HCONF_ATTR_OUTPUT_DATASET);
  }

  @Override
  public void checkOutputSpecs(final JobContext context) throws IOException, InterruptedException {
    // TODO: validate out types? Or this is ensured by configuring job in "internal" code (i.e. not in user code)
  }

  @Override
  public OutputCommitter getOutputCommitter(final TaskAttemptContext context) throws IOException, InterruptedException {
    return new DataSetOutputCommitter();
  }
}
