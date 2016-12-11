/*
 * Copyright © 2016 Cask Data, Inc.
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
import com.google.common.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * A RecordWriter that allows writing dynamically to multiple partitions of a PartitionedFileSet.
 */
abstract class DynamicPartitionerWriterWrapper<K, V> extends RecordWriter<K, V> {

  private TaskAttemptContext job;
  private String outputName;
  private Partitioning partitioning;
  private FileOutputFormat<K, V> fileOutputFormat;

  @SuppressWarnings("unchecked")
  DynamicPartitioner<K, V> dynamicPartitioner;
  BasicMapReduceTaskContext<K, V> taskContext;

  DynamicPartitionerWriterWrapper(TaskAttemptContext job) {
    this.job = job;
    this.outputName = DynamicPartitioningOutputFormat.getOutputName(job);

    Configuration configuration = job.getConfiguration();
    Class<? extends DynamicPartitioner> partitionerClass = configuration
      .getClass(PartitionedFileSetArguments.DYNAMIC_PARTITIONER_CLASS_NAME, null, DynamicPartitioner.class);
    this.dynamicPartitioner = new InstantiatorFactory(false).get(TypeToken.of(partitionerClass)).create();

    MapReduceClassLoader classLoader = MapReduceClassLoader.getFromConfiguration(configuration);
    this.taskContext = classLoader.getTaskContextProvider().get(job);

    String outputDatasetName = configuration.get(Constants.Dataset.Partitioned.HCONF_ATTR_OUTPUT_DATASET);
    PartitionedFileSet outputDataset = taskContext.getDataset(outputDatasetName);
    this.partitioning = outputDataset.getPartitioning();

    this.dynamicPartitioner.initialize(taskContext);
  }

  // returns a TaskAttemptContext whose configuration will reflect the specified partitionKey's path as the output path
  TaskAttemptContext getKeySpecificContext(PartitionKey partitionKey) throws IOException {
    String relativePath = PartitionedFileSetDataset.getOutputPath(partitionKey, partitioning);
    String finalPath = relativePath + "/" + outputName;
    return getTaskAttemptContext(job, finalPath);
  }

  /**
   * @return A RecordWriter object for the given TaskAttemptContext (configured for a particular file name).
   * @throws IOException
   */
  RecordWriter<K, V> getBaseRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
    return getFileOutputFormat(job).getRecordWriter(job);
  }

  // returns a TaskAttemptContext whose configuration will reflect the specified newOutputName as the output path
  private TaskAttemptContext getTaskAttemptContext(TaskAttemptContext context,
                                                   String newOutputName) throws IOException {
    Job job = new Job(context.getConfiguration());
    DynamicPartitioningOutputFormat.setOutputName(job, newOutputName);
    // CDAP-4806 We must set this parameter in addition to calling FileOutputFormat#setOutputName, because
    // AvroKeyOutputFormat/AvroKeyValueOutputFormat use a different parameter for the output name than FileOutputFormat.
    if (isAvroOutputFormat(getFileOutputFormat(context))) {
      job.getConfiguration().set("avro.mo.config.namedOutput", newOutputName);
    }

    Path jobOutputPath = DynamicPartitioningOutputFormat.createJobSpecificPath(FileOutputFormat.getOutputPath(job),
                                                                               context);
    DynamicPartitioningOutputFormat.setOutputPath(job, jobOutputPath);

    return new TaskAttemptContextImpl(job.getConfiguration(), context.getTaskAttemptID());
  }

  private FileOutputFormat<K, V> getFileOutputFormat(TaskAttemptContext job) {
    if (fileOutputFormat == null) {
      Class<? extends FileOutputFormat> delegateOutputFormat = job.getConfiguration()
        .getClass(Constants.Dataset.Partitioned.HCONF_ATTR_OUTPUT_FORMAT_CLASS_NAME, null, FileOutputFormat.class);

      @SuppressWarnings("unchecked")
      FileOutputFormat<K, V> fileOutputFormat =
        ReflectionUtils.newInstance(delegateOutputFormat, job.getConfiguration());
      this.fileOutputFormat = fileOutputFormat;
    }
    return fileOutputFormat;
  }

  private static boolean isAvroOutputFormat(FileOutputFormat fileOutputFormat) {
    String className = fileOutputFormat.getClass().getName();
    // use class name String in order avoid having a dependency on the Avro libraries here
    return "org.apache.avro.mapreduce.AvroKeyOutputFormat".equals(className)
      || "org.apache.avro.mapreduce.AvroKeyValueOutputFormat".equals(className);
  }
}
