/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetDataset;
import co.cask.cdap.internal.app.runtime.batch.BasicMapReduceTaskContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

import java.io.IOException;

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

  /**
   * Create a composite record writer that can write key/value data to different output files.
   *
   * @return a composite record writer
   * @throws IOException
   */
  @Override
  public RecordWriter<K, V> getRecordWriter(final TaskAttemptContext job) throws IOException {
    boolean concurrencyAllowed =
      job.getConfiguration().getBoolean(PartitionedFileSetArguments.DYNAMIC_PARTITIONER_ALLOW_CONCURRENCY, true);
    if (concurrencyAllowed) {
      return new MultiWriter<>(job);
    } else {
      return new SingleWriter<>(job);
    }
  }
  // suffixes a Path with a job-specific string
  static Path createJobSpecificPath(Path path, JobContext jobContext) {
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

  // workaround, since FileOutputFormat.getOutputName is protected accessibility.
  static String getOutputName(TaskAttemptContext job) {
    return FileOutputFormat.getOutputName(job);
  }

  // workaround, since FileOutputFormat.setOutputPath is protected accessibility.
  static void setOutputPath(Path jobOutputPath, Job job) {
    FileOutputFormat.setOutputPath(job, jobOutputPath);
  }

  // workaround, since FileOutputFormat.setOutputName is protected accessibility.
  static void setOutputName(Job job, String newOutputName) {
    FileOutputFormat.setOutputName(job, newOutputName);
  }
}
