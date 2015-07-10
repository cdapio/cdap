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

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetInputSplit;
import co.cask.cdap.internal.app.runtime.spark.ExecutionSparkContext;
import co.cask.cdap.internal.app.runtime.spark.SparkContextProvider;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An {@link InputFormat} for {@link Spark} jobs that reads from {@link Dataset}.
 *
 * @param <KEY>   Type of key.
 * @param <VALUE> Type of value.
 *                TODO: Refactor this and MapReduce DatasetInputFormat
 */
public final class SparkDatasetInputFormat<KEY, VALUE> extends InputFormat<KEY, VALUE> {

  private static final Gson GSON = new Gson();
  private static final Type ARGS_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  // Set of job configuration keys for setting configurations into job conf.
  private static final String INPUT_DATASET_NAME = "input.spark.dataset.name";
  private static final String INPUT_DATASET_ARGS = "input.spark.dataset.args";

  /**
   * Sets the configurations for the dataset name and configurations for the input format.
   */
  public static void setDataset(Configuration configuration, String dataset, Map<String, String> arguments) {
    configuration.set(INPUT_DATASET_NAME, dataset);
    configuration.set(INPUT_DATASET_ARGS, GSON.toJson(arguments, ARGS_TYPE));
  }

  @Override
  public List<InputSplit> getSplits(final JobContext context) throws IOException, InterruptedException {
    ExecutionSparkContext sparkContext = SparkContextProvider.getSparkContext();

    Configuration configuration = context.getConfiguration();
    Map<String, String> arguments = GSON.fromJson(configuration.get(INPUT_DATASET_ARGS), ARGS_TYPE);
    BatchReadable<?, ?> batchReadable = sparkContext.getBatchReadable(configuration.get(INPUT_DATASET_NAME), arguments);

    List<Split> splits = batchReadable.getSplits();
    List<InputSplit> list = new ArrayList<>(splits.size());
    for (Split split : splits) {
      list.add(new DataSetInputSplit(split));
    }
    return list;
  }

  @Override
  public RecordReader<KEY, VALUE> createRecordReader(InputSplit split,
                                                     TaskAttemptContext context) throws IOException {
    DataSetInputSplit inputSplit = (DataSetInputSplit) split;
    BatchReadable<KEY, VALUE> batchReadable = getBatchReadable(context.getConfiguration());
    SplitReader<KEY, VALUE> splitReader = batchReadable.createSplitReader(inputSplit.getSplit());

    return new DatasetRecordReader<>(splitReader);
  }

  private BatchReadable<KEY, VALUE> getBatchReadable(Configuration configuration) {
    Map<String, String> args = GSON.fromJson(configuration.get(INPUT_DATASET_ARGS), ARGS_TYPE);
    return SparkContextProvider.getSparkContext().getBatchReadable(configuration.get(INPUT_DATASET_NAME), args);
  }
}
