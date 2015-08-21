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

import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetOutputCommitter;
import co.cask.cdap.internal.app.runtime.spark.SparkContextProvider;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * An {@link OutputFormat} for writing into dataset.
 *
 * @param <KEY>   Type of key.
 * @param <VALUE> Type of value.
 *                TODO: Refactor this OutputFormat and MapReduce OutputFormat
 */
public final class SparkDatasetOutputFormat<KEY, VALUE> extends OutputFormat<KEY, VALUE> {

  private static final Gson GSON = new Gson();
  private static final Type ARGS_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private static final String OUTPUT_DATASET_NAME = "output.spark.dataset.name";
  private static final String OUTPUT_DATASET_ARGS = "output.spark.dataset.args";

  /**
   * Sets the configurations for the dataset name and configurations for the output format.
   */
  public static void setDataset(Configuration configuration, String dataset, Map<String, String> arguments) {
    configuration.set(OUTPUT_DATASET_NAME, dataset);
    configuration.set(OUTPUT_DATASET_ARGS, GSON.toJson(arguments, ARGS_TYPE));
  }

  @Override
  public RecordWriter<KEY, VALUE> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    CloseableBatchWritable<KEY, VALUE> batchWritable = getBatchWritable(context.getConfiguration());
    return new DatasetRecordWriter<>(batchWritable);
  }


  @Override
  public void checkOutputSpecs(final JobContext context) throws IOException, InterruptedException {
    // TODO: validate out types? Or this is ensured by configuring job in "internal" code (i.e. not in user code)
  }

  @Override
  public OutputCommitter getOutputCommitter(final TaskAttemptContext context) throws IOException, InterruptedException {
    return new DataSetOutputCommitter();
  }


  private <K, V> CloseableBatchWritable<K, V> getBatchWritable(Configuration configuration) {
    Map<String, String> args = GSON.fromJson(configuration.get(OUTPUT_DATASET_ARGS), ARGS_TYPE);
    return SparkContextProvider.getSparkContext().getBatchWritable(configuration.get(OUTPUT_DATASET_NAME), args);
  }
}
