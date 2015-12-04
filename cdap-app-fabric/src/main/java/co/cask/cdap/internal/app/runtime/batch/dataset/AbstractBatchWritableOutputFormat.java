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

import co.cask.cdap.api.data.batch.BatchWritable;
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
 * An abstract base implementation of {@link OutputFormat} for writing to {@link BatchWritable} from batch job.
 *
 * @param <KEY> type of the key
 * @param <VALUE> type of the value
 */
public abstract class AbstractBatchWritableOutputFormat<KEY, VALUE> extends OutputFormat<KEY, VALUE> {

  private static final Gson GSON = new Gson();
  private static final Type DATASET_ARGS_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private static final String DATASET_NAME = "output.datasetoutputformat.dataset.name";
  private static final String DATASET_ARGS = "output.datasetoutputformat.dataset.args";

  /**
   * Sets dataset information into the given {@link Configuration}.
   *
   * @param hConf       configuration to modify
   * @param datasetName name of the dataset
   * @param datasetArgs arguments for the dataset
   */
  public static void setDataset(Configuration hConf, String datasetName, Map<String, String> datasetArgs) {
    hConf.set(DATASET_NAME, datasetName);
    hConf.set(DATASET_ARGS, GSON.toJson(datasetArgs, DATASET_ARGS_TYPE));
  }

  @Override
  public RecordWriter<KEY, VALUE> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    String datasetName = conf.get(DATASET_NAME);
    Map<String, String> datasetArgs = GSON.fromJson(conf.get(DATASET_ARGS), DATASET_ARGS_TYPE);
    return new BatchWritableRecordWriter<>(createBatchWritable(context, datasetName, datasetArgs));
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    Configuration hConf = context.getConfiguration();
    if (hConf.get(DATASET_NAME) == null || hConf.get(DATASET_ARGS) == null) {
      throw new IOException("Dataset configurations are missing in the job configuration");
    }
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new NoopOutputCommitter();
  }

  /**
   * Subclass needs to implementation this method to return a {@link BatchWritable} for writing records to
   * the given dataset.
   *
   * @param context the hadoop task context
   * @param datasetName name of the dataset to write to
   * @param datasetArgs arguments of the dataset to write to
   */
  protected abstract CloseableBatchWritable<KEY, VALUE> createBatchWritable(TaskAttemptContext context,
                                                                            String datasetName,
                                                                            Map<String, String> datasetArgs);

  /**
   * Implementation of {@link RecordWriter} to write through a {@link CloseableBatchWritable}.
   */
  private static final class BatchWritableRecordWriter<K, V> extends RecordWriter<K, V> {

    private final CloseableBatchWritable<K, V> delegate;

    private BatchWritableRecordWriter(CloseableBatchWritable<K, V> delegate) {
      this.delegate = delegate;
    }

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      delegate.write(key, value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      delegate.close();
    }
  }

  /**
   * A no-op implementation of {@link OutputCommitter}.
   */
  private static final class NoopOutputCommitter extends OutputCommitter {
    @Override
    public void setupJob(final JobContext jobContext) throws IOException {
      // DO NOTHING, see needsTaskCommit() comment
    }

    @Override
    public boolean needsTaskCommit(final TaskAttemptContext taskContext) throws IOException {
      // Don't do commit of individual task work. Work is committed on job level. Ops are flushed on RecordWriter.close.
      return false;
    }

    @Override
    public void setupTask(final TaskAttemptContext taskContext) throws IOException {
      // DO NOTHING, see needsTaskCommit() comment
    }

    @Override
    public void commitTask(final TaskAttemptContext taskContext) throws IOException {
      // DO NOTHING, see needsTaskCommit() comment
    }

    @Override
    public void abortTask(final TaskAttemptContext taskContext) throws IOException {
      // DO NOTHING, see needsTaskCommit() comment
    }
  }
}
