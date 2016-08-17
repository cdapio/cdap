/*
 * Copyright © 2015 Cask Data, Inc.
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
import javax.annotation.Nullable;

/**
 * An abstract {@link InputFormat} implementation that reads from {@link BatchReadable}.
 * @param <KEY> Type of key.
 * @param <VALUE> Type of value.
 */
public abstract class AbstractBatchReadableInputFormat<KEY, VALUE> extends InputFormat<KEY, VALUE> {

  private static final Gson GSON = new Gson();
  private static final Type DATASET_ARGS_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  // Keys for storing values in Hadoop Configuration
  private static final String DATASET_NAMESPACE = "input.datasetinputformat.dataset.namespace";
  private static final String DATASET_NAME = "input.datasetinputformat.dataset.name";
  private static final String DATASET_ARGS = "input.datasetinputformat.dataset.args";
  private static final String SPLITS = "input.datasetinputformat.splits";

  /**
   * Sets dataset and splits information into the given {@link Configuration}.
   *
   * @param hConf            configuration to modify
   * @param datasetNamespace namespace of the dataset
   * @param datasetName      name of the dataset
   * @param datasetArguments arguments for the dataset
   * @param splits           list of splits on the dataset
   * @throws IOException
   */
  public static void setDatasetSplits(Configuration hConf, @Nullable String datasetNamespace,
                                      String datasetName, Map<String, String> datasetArguments,
                                      List<Split> splits) throws IOException {
    if (datasetNamespace != null) {
      hConf.set(DATASET_NAMESPACE, datasetNamespace);
    }
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

    String datasetNamespace = conf.get(DATASET_NAMESPACE);
    String datasetName = conf.get(DATASET_NAME);
    Map<String, String> datasetArgs = GSON.fromJson(conf.get(DATASET_ARGS), DATASET_ARGS_TYPE);

    @SuppressWarnings("unchecked")
    BatchReadable<KEY, VALUE> batchReadable = createBatchReadable(context, datasetNamespace, datasetName, datasetArgs);
    SplitReader<KEY, VALUE> splitReader = batchReadable.createSplitReader(inputSplit.getSplit());
    return new SplitReaderRecordReader<>(splitReader);
  }

  /**
   * Subclass needs to implementation this method to return a {@link BatchReadable} for reading records from
   * the given dataset.
   *
   * @param context the hadoop task context
   * @param datasetName name of the dataset to read from
   * @param datasetArgs arguments of the dataset to read from
   */
  protected abstract BatchReadable<KEY, VALUE> createBatchReadable(TaskAttemptContext context,
                                                                   String datasetName, Map<String, String> datasetArgs);

  /**
   * Subclass needs to implementation this method to return a {@link BatchReadable} for reading records from
   * the given dataset from another namespace.
   *
   * @param context the hadoop task context
   * @param datasetNamespace namesoace of the dataset
   * @param datasetName name of the dataset to read from
   * @param datasetArgs arguments of the dataset to read from
   */
  protected abstract BatchReadable<KEY, VALUE> createBatchReadable(TaskAttemptContext context,
                                                                   @Nullable String datasetNamespace,
                                                                   String datasetName, Map<String, String> datasetArgs);


  /**
   * Implementation of {@link RecordReader} by delegating to the underlying {@link SplitReader}.
   *
   * @param <KEY> type of key returned by this reader
   * @param <VALUE> type of value returned by this reader
   */
  private static final class SplitReaderRecordReader<KEY, VALUE> extends RecordReader<KEY, VALUE> {

    private final SplitReader<KEY, VALUE> splitReader;

    SplitReaderRecordReader(final SplitReader<KEY, VALUE> splitReader) {
      this.splitReader = splitReader;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
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
      splitReader.close();
    }
  }
}
