/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.internal.app.runtime.batch.BasicMapReduceTaskContext;
import co.cask.cdap.internal.app.runtime.batch.MapReduceClassLoader;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
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
 * An {@link InputFormat} that reads from dataset.
 * @param <KEY> Type of key.
 * @param <VALUE> Type of value.
 */
public final class DataSetInputFormat<KEY, VALUE> extends InputFormat<KEY, VALUE> {

  private static final Gson GSON = new Gson();
  private static final Type DATASET_ARGS_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  // Keys for storing values in Hadoop Configuration
  private static final String DATASET_NAME = "input.datasetinputformat.dataset.name";
  private static final String DATASET_ARGS = "input.datasetinputformat.dataset.args";
  private static final String SPLITS = "input.datasetinputformat.splits";

  /**
   * Sets dataset and splits information into the given {@link Configuration}.
   *
   * @param hConf configuration to modify
   * @param datasetName name of the dataset
   * @param datasetArguments arguments for the dataset
   * @param splits list of splits on the dataset
   * @throws IOException
   */
  public static void setDatasetSplits(Configuration hConf,
                                      String datasetName, Map<String, String> datasetArguments,
                                      List<Split> splits) throws IOException {
    hConf.set(DATASET_NAME, datasetName);
    hConf.set(DATASET_ARGS, GSON.toJson(datasetArguments, DATASET_ARGS_TYPE));

    // Encode the list of splits with size followed by that many of DataSetInputSplit objects.
    ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput();
    dataOutput.writeInt(splits.size());
    for (Split split : splits) {
      new DataSetInputSplit(split).write(dataOutput);
    }
    hConf.set(SPLITS, Bytes.toStringBinary(dataOutput.toByteArray()));
  }

  @Override
  public List<InputSplit> getSplits(final JobContext context) throws IOException, InterruptedException {
    // Decode splits from Configuration
    String splitsConf = context.getConfiguration().get(SPLITS);
    if (splitsConf == null) {
      throw new IOException("No input splits available from job configuration.");
    }
    ByteArrayDataInput dataInput = ByteStreams.newDataInput(Bytes.toBytesBinary(splitsConf));
    int size = dataInput.readInt();
    List<InputSplit> splits = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      DataSetInputSplit inputSplit = new DataSetInputSplit();
      inputSplit.readFields(dataInput);
      splits.add(inputSplit);
    }
    return splits;
  }

  @Override
  public RecordReader<KEY, VALUE> createRecordReader(InputSplit split,
                                                     TaskAttemptContext context)
    throws IOException, InterruptedException {

    DataSetInputSplit inputSplit = (DataSetInputSplit) split;

    Configuration conf = context.getConfiguration();
    MapReduceClassLoader classLoader = MapReduceClassLoader.getFromConfiguration(conf);
    BasicMapReduceTaskContext taskContext = classLoader.getTaskContextProvider().get(context);

    String datasetName = conf.get(DATASET_NAME);
    Map<String, String> datasetArgs = GSON.fromJson(conf.get(DATASET_ARGS), DATASET_ARGS_TYPE);

    Dataset dataset = taskContext.getDataset(datasetName, datasetArgs);
    // Must be BatchReadable.
    Preconditions.checkArgument(dataset instanceof BatchReadable, "Dataset '%s' is not a BatchReadable.", datasetName);

    @SuppressWarnings("unchecked")
    BatchReadable<KEY, VALUE> batchReadable = (BatchReadable<KEY, VALUE>) dataset;
    SplitReader<KEY, VALUE> splitReader = batchReadable.createSplitReader(inputSplit.getSplit());

    // the record reader now owns the context and will close it
    return new DataSetRecordReader<>(splitReader, taskContext);
  }
}
