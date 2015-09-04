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
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetDataset;
import co.cask.cdap.internal.app.runtime.batch.BasicMapReduceTaskContext;
import co.cask.cdap.internal.app.runtime.batch.MapReduceTaskContextProvider;
import com.google.common.reflect.TypeToken;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;
import java.util.TreeMap;

/**
 * This class extends the FileOutputFormat and allows writing dynamically to multiple partitions of a PartitionedFileSet
 * Dataset.
 *
 * @param <K> Type of key
 * @param <V> Type of value
 */
public class DynamicPartitioningOutputFormat<K, V> extends FileOutputFormat<K, V> {
//      TODO: Hive doesn't allow addPartition w/o the partition location existing. verify this?
  public static final String HCONF_ATTR_OUTPUT_DATASET = "output.dataset.name";

  // TODO: keep the limit to only FileOutputFormat and FileOutputCommitter?
  private FileOutputFormat<K, V> fileOutputFormat;
  private FileOutputCommitter committer;

  /**
   * Create a composite record writer that can write key/value data to different output files.
   *
   * @return a composite record writer
   * @throws IOException
   */
  @Override
  public RecordWriter<K, V> getRecordWriter(final TaskAttemptContext job) throws IOException {

    final String outputName = FileOutputFormat.getOutputName(job);

    Class<? extends DynamicPartitioner> className = job.getConfiguration()
      .getClass(PartitionedFileSetArguments.DYNAMIC_PARTITIONER_CLASS_NAME, null, DynamicPartitioner.class);

    @SuppressWarnings("unchecked")
    final DynamicPartitioner<K, V> dynamicPartitioner =
      new InstantiatorFactory(false).get(TypeToken.of(className)).create();

    final BasicMapReduceTaskContext<K, V> mrTaskContext = new MapReduceTaskContextProvider(job, null).get();
    dynamicPartitioner.initialize(mrTaskContext);

    return new RecordWriter<K, V>() {

      // a cache storing the record writers for different output files.
      TreeMap<String, RecordWriter<K, V>> recordWriters = new TreeMap<>();

      public void write(K key, V value) throws IOException, InterruptedException {
        PartitionKey partitionKey = dynamicPartitioner.getPartitionKey(key, value);
        String relativePath = PartitionedFileSetDataset.getOutputPath(partitionKey);
        String finalPath = relativePath + "/" + outputName;

        RecordWriter<K, V> rw = this.recordWriters.get(finalPath);
        if (rw == null) {
          // if we don't have the record writer yet for the final path, create one and add it to the cache
          rw = getBaseRecordWriter(getTaskAttemptContext(job, finalPath));
          this.recordWriters.put(finalPath, rw);
        }
        rw.write(key, value);
      }

      @Override
      public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        try {
          for (RecordWriter<K, V> recordWriter : this.recordWriters.values()) {
            recordWriter.close(context);
          }
          this.recordWriters.clear();

          mrTaskContext.flushOperations();
        } catch (Exception e) {
          throw new IOException(e);
        } finally {
          mrTaskContext.close();
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
      // TODO: use the PFS's OutputFormat (encode in conf)
      fileOutputFormat = new TextOutputFormat<>();
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
    if (committer == null) {
      final Path jobSpecificOutputPath = createJobSpecificPath(getOutputPath(context), context);
      committer = new DynamicPartitioningOutputCommitter(jobSpecificOutputPath, context);
    }
    return committer;
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

    // additionally check that output dataset has been set in conf
    if (job.getConfiguration().get(HCONF_ATTR_OUTPUT_DATASET) == null) {
      throw new InvalidJobConfException("Output dataset not set");
    }
  }
}
