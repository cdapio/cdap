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

package co.cask.cdap.internal.app.runtime.batch.dataset.partitioned;

import co.cask.cdap.api.dataset.lib.DynamicPartitioner;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetDataset;
import co.cask.cdap.internal.app.runtime.batch.BasicMapReduceTaskContext;
import co.cask.cdap.internal.app.runtime.batch.MapReduceClassLoader;
import co.cask.cdap.internal.app.runtime.batch.dataset.MultipleOutputs;
import com.google.common.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class extends the FileOutputFormat and allows writing dynamically to multiple partitions of a PartitionedFileSet
 * Dataset.
 *
 * This class is used in {@link PartitionedFileSetDataset} and is referred to this class by name because data-fabric
 * doesn't depends on app-fabric, while this class needs access to app-fabric class {@link BasicMapReduceTaskContext}.
 *
 * @param <K> Type of key
 * @param <V> Type of value
 */
@SuppressWarnings("unused")
public class DynamicPartitioningOutputFormat<K, V> extends FileOutputFormat<K, V> {

  private FileOutputFormat<K, V> fileOutputFormat;

  /**
   * Create a composite record writer that can write key/value data to different output files.
   *
   * @return a composite record writer
   * @throws IOException
   */
  @Override
  public RecordWriter<K, V> getRecordWriter(final TaskAttemptContext job) throws IOException {
    final String outputName = FileOutputFormat.getOutputName(job);

    Configuration configuration = job.getConfiguration();
    Class<? extends DynamicPartitioner> partitionerClass = configuration
      .getClass(PartitionedFileSetArguments.DYNAMIC_PARTITIONER_CLASS_NAME, null, DynamicPartitioner.class);

    @SuppressWarnings("unchecked")
    final DynamicPartitioner<K, V> dynamicPartitioner =
      new InstantiatorFactory(false).get(TypeToken.of(partitionerClass)).create();

    MapReduceClassLoader classLoader = MapReduceClassLoader.getFromConfiguration(configuration);
    final BasicMapReduceTaskContext<K, V> taskContext = classLoader.getTaskContextProvider().get(job);

    String outputDatasetName = configuration.get(Constants.Dataset.Partitioned.HCONF_ATTR_OUTPUT_DATASET);
    PartitionedFileSet outputDataset = taskContext.getDataset(outputDatasetName);
    final Partitioning partitioning = outputDataset.getPartitioning();

    dynamicPartitioner.initialize(taskContext);

    return new RecordWriter<K, V>() {

      // a cache storing the record writers for different output files.
      Map<PartitionKey, RecordWriter<K, V>> recordWriters = new HashMap<>();

      public void write(K key, V value) throws IOException, InterruptedException {
        PartitionKey partitionKey = dynamicPartitioner.getPartitionKey(key, value);
        RecordWriter<K, V> rw = this.recordWriters.get(partitionKey);
        if (rw == null) {
          String relativePath = PartitionedFileSetDataset.getOutputPath(partitionKey, partitioning);
          String finalPath = relativePath + "/" + outputName;

          // if we don't have the record writer yet for the final path, create one and add it to the cache
          rw = getBaseRecordWriter(getTaskAttemptContext(job, finalPath));
          this.recordWriters.put(partitionKey, rw);
        }
        rw.write(key, value);
      }

      @Override
      public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        try {
          List<RecordWriter<?, ?>> recordWriters = new ArrayList<>();
          recordWriters.addAll(this.recordWriters.values());
          MultipleOutputs.closeRecordWriters(recordWriters, context);

          taskContext.flushOperations();
        } catch (Exception e) {
          throw new IOException(e);
        } finally {
          dynamicPartitioner.destroy();
        }
      }
    };
  }

  private static TaskAttemptContext getTaskAttemptContext(TaskAttemptContext context,
                                                          String newOutputName) throws IOException {
    Job job = new Job(context.getConfiguration());
    FileOutputFormat.setOutputName(job, newOutputName);

    Path jobOutputPath = createJobSpecificPath(FileOutputFormat.getOutputPath(job), context);
    FileOutputFormat.setOutputPath(job, jobOutputPath);

    return new TaskAttemptContextImpl(job.getConfiguration(), context.getTaskAttemptID());
  }

  /**
   * @return A RecordWriter object over the given file
   * @throws IOException
   */
  protected RecordWriter<K, V> getBaseRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
    if (fileOutputFormat == null) {
      Class<? extends FileOutputFormat> delegateOutputFormat = job.getConfiguration()
        .getClass(Constants.Dataset.Partitioned.HCONF_ATTR_OUTPUT_FORMAT_CLASS_NAME, null, FileOutputFormat.class);

      @SuppressWarnings("unchecked")
      FileOutputFormat<K, V> fileOutputFormat =
        new InstantiatorFactory(false).get(TypeToken.of(delegateOutputFormat)).create();
      this.fileOutputFormat = fileOutputFormat;
    }
    return fileOutputFormat.getRecordWriter(job);
  }

  // suffixes a Path with a job-specific string
  private static Path createJobSpecificPath(Path path, JobContext jobContext) {
    String outputPathSuffix = "_temporary_" + jobContext.getJobID().getId();
    return new Path(path, outputPathSuffix);
  }

  @Override
  public synchronized FileOutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
    final Path jobSpecificOutputPath = createJobSpecificPath(getOutputPath(context), context);
    return new DynamicPartitioningOutputCommitter(jobSpecificOutputPath, context);
  }

  @Override
  public void checkOutputSpecs(JobContext job) throws IOException {
    // Ensure that the output directory is set and not already there
    Path outDir = getOutputPath(job);
    if (outDir == null) {
      throw new InvalidJobConfException("Output directory not set.");
    }

    // get delegation token for outDir's file system
    TokenCache.obtainTokensForNamenodes(job.getCredentials(),
                                        new Path[]{outDir}, job.getConfiguration());

    // we permit multiple jobs writing to the same output directory. We handle this by each one writing to distinct
    // paths within that directory. See createJobSpecificPath method and usages of it.

    // additionally check that output dataset and dynamic partitioner class name has been set in conf
    if (job.getConfiguration().get(Constants.Dataset.Partitioned.HCONF_ATTR_OUTPUT_DATASET) == null) {
      throw new InvalidJobConfException("The job configuration does not contain required property: "
                                          + Constants.Dataset.Partitioned.HCONF_ATTR_OUTPUT_DATASET);
    }

    Class<? extends DynamicPartitioner> partitionerClass = job.getConfiguration()
      .getClass(PartitionedFileSetArguments.DYNAMIC_PARTITIONER_CLASS_NAME, null, DynamicPartitioner.class);
    if (partitionerClass == null) {
      throw new InvalidJobConfException("The job configuration does not contain required property: "
                                          + PartitionedFileSetArguments.DYNAMIC_PARTITIONER_CLASS_NAME);
    }

    Class<? extends FileOutputFormat> delegateOutputFormatClass = job.getConfiguration()
      .getClass(Constants.Dataset.Partitioned.HCONF_ATTR_OUTPUT_FORMAT_CLASS_NAME, null, FileOutputFormat.class);
    if (delegateOutputFormatClass == null) {
      throw new InvalidJobConfException("The job configuration does not contain required property: "
                                          + Constants.Dataset.Partitioned.HCONF_ATTR_OUTPUT_FORMAT_CLASS_NAME);
    }
  }
}
