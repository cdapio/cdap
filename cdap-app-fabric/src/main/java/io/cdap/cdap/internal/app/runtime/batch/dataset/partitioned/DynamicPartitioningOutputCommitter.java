/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.common.conf.ConfigurationUtil;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetDataset;
import co.cask.cdap.internal.app.runtime.batch.BasicMapReduceTaskContext;
import co.cask.cdap.internal.app.runtime.batch.MapReduceClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An OutputCommitter which creates partitions in a configured PartitionedFileSet dataset for all of the partitions
 * that were written to by a DynamicPartitioningOutputFormat
 * It enables this by having each job write to a job-specific temporary path within that output directory.
 * Then, upon commitJob, it moves the files to the final, parent directory if the final output directories do not
 * already exist.
 */
public class DynamicPartitioningOutputCommitter extends FileOutputCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitioningOutputCommitter.class);

  private final TaskAttemptContext taskContext;
  private final Path jobSpecificOutputPath;

  private PartitionedFileSetDataset outputDataset;
  private Map<String, PartitionKey> partitionsToAdd;

  // Note that the outputPath passed in is treated as a temporary directory.
  // The commitJob method moves the files from within this directory to an parent (final) directory.
  // The cleanupJob method removes this directory.
  public DynamicPartitioningOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    this.taskContext = context;
    this.jobSpecificOutputPath = outputPath;
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    Configuration configuration = context.getConfiguration();
    MapReduceClassLoader classLoader = MapReduceClassLoader.getFromConfiguration(configuration);
    BasicMapReduceTaskContext taskContext = classLoader.getTaskContextProvider().get(this.taskContext);

    String outputDatasetName = configuration.get(Constants.Dataset.Partitioned.HCONF_ATTR_OUTPUT_DATASET);
    outputDataset = taskContext.getDataset(outputDatasetName);
    DynamicPartitioner.PartitionWriteOption partitionWriteOption = DynamicPartitioner.PartitionWriteOption.valueOf(
      configuration.get(PartitionedFileSetArguments.DYNAMIC_PARTITIONER_WRITE_OPTION));
    Partitioning partitioning = outputDataset.getPartitioning();

    partitionsToAdd = new HashMap<>();
    // Go over all files in the temporary directory and keep track of partitions to add for them
    FileStatus[] allCommittedTaskPaths = getAllCommittedTaskPaths(context);
    for (FileStatus committedTaskPath : allCommittedTaskPaths) {
      FileSystem fs = committedTaskPath.getPath().getFileSystem(configuration);
      RemoteIterator<LocatedFileStatus> fileIter = fs.listFiles(committedTaskPath.getPath(), true);
      while (fileIter.hasNext()) {
        Path path = fileIter.next().getPath();
        String relativePath = getRelative(committedTaskPath.getPath(), path);

        int lastPathSepIdx = relativePath.lastIndexOf(Path.SEPARATOR);
        if (lastPathSepIdx == -1) {
          // this shouldn't happen because each relative path should consist of at least one partition key and
          // the output file name
          LOG.warn("Skipping path '{}'. It's relative path '{}' has fewer than two parts", path, relativePath);
          continue;
        }
        // relativePath = "../key1/key2/part-m-00000"
        // relativeDir = "../key1/key2"
        // fileName = "part-m-00000"
        String relativeDir = relativePath.substring(0, lastPathSepIdx);

        Path finalDir = new Path(FileOutputFormat.getOutputPath(context), relativeDir);
        if (partitionWriteOption == DynamicPartitioner.PartitionWriteOption.CREATE) {
          if (fs.exists(finalDir)) {
            throw new FileAlreadyExistsException("Final output path already exists: " + finalDir);
          }
        }
        PartitionKey partitionKey = getPartitionKey(partitioning, relativeDir);
        partitionsToAdd.put(relativeDir, partitionKey);
      }
    }

    // need to remove any existing partitions, before moving temporary content to final output
    if (partitionWriteOption == DynamicPartitioner.PartitionWriteOption.CREATE_OR_OVERWRITE) {
      for (Map.Entry<String, PartitionKey> entry : partitionsToAdd.entrySet()) {
        if (outputDataset.getPartition(entry.getValue()) != null) {
          // this allows reinstating the existing files if there's a rollback.
          // alternative is to simply remove the files within the partition's location
          // upside to that is easily avoiding explore operations. one downside is that metadata is not removed then
          outputDataset.dropPartition(entry.getValue());
        }
      }
    }

    // We need to copy to the parent of the FileOutputFormat's outputDir, since we added a _temporary_jobId suffix to
    // the original outputDir.
    Path finalOutput = FileOutputFormat.getOutputPath(context);
    FileContext fc = FileContext.getFileContext(configuration);
    // the finalOutput path doesn't have scheme or authority (but 'from' does)
    finalOutput = fc.makeQualified(finalOutput);
    for (FileStatus from : getAllCommittedTaskPaths(context)) {
      mergePaths(fc, from, finalOutput);
    }


    // compute the metadata to be written to every output partition
    Map<String, String> metadata =
      ConfigurationUtil.getNamedConfigurations(this.taskContext.getConfiguration(),
                                               PartitionedFileSetArguments.OUTPUT_PARTITION_METADATA_PREFIX);

    boolean allowAppend = partitionWriteOption == DynamicPartitioner.PartitionWriteOption.CREATE_OR_APPEND;
    // create all the necessary partitions
    for (Map.Entry<String, PartitionKey> entry : partitionsToAdd.entrySet()) {
      outputDataset.addPartition(entry.getValue(), entry.getKey(), metadata, true, allowAppend);
    }

    // delete the job-specific _temporary folder
    cleanupJob(context);

    // mark all the final output paths with a _SUCCESS file, if configured to do so (default = true)
    if (configuration.getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)) {
      for (String relativePath : partitionsToAdd.keySet()) {
        Path pathToMark = new Path(finalOutput, relativePath);

        createOrUpdate(fc, new Path(pathToMark, SUCCEEDED_FILE_NAME));
        // also create a _SUCCESS-<RunId>, if allowing append
        if (allowAppend) {
          createOrUpdate(fc, new Path(pathToMark, SUCCEEDED_FILE_NAME + "-" + taskContext.getProgramRunId().getRun()));
        }
      }
    }
  }

  private void createOrUpdate(FileContext fc, Path markerPath) throws IOException {
    // Similar to FileSystem#createNewFile(Path)
    fc.create(markerPath, EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND)).close();
  }

  private PartitionKey getPartitionKey(Partitioning partitioning, String relativePath) {
    List<String> pathParts = Arrays.asList(relativePath.split(Path.SEPARATOR));

    if (pathParts.size() != partitioning.getFields().size()) {
      throw new IllegalArgumentException(
        String.format("relativePath '%s' does not have same number of components as partitioning '%s",
                      relativePath, partitioning));
    }

    PartitionKey.Builder builder = PartitionKey.builder();
    int i = 0;
    for (Map.Entry<String, Partitioning.FieldType> entry : partitioning.getFields().entrySet()) {
      String keyName = entry.getKey();
      Comparable keyValue = entry.getValue().parse(pathParts.get(i));
      builder.addField(keyName, keyValue);
      i++;
    }
    return builder.build();
  }

  @Override
  public void cleanupJob(JobContext context) throws IOException {
    FileSystem fs = jobSpecificOutputPath.getFileSystem(context.getConfiguration());
    fs.delete(jobSpecificOutputPath, true);
  }

  @Override
  public void abortJob(JobContext context, JobStatus.State state) throws IOException {
    // if this is set, then commitJob() was called already and may have created partitions:
    // We want to rollback these partitions.
    if (outputDataset != null) {
      try {
        try {
          outputDataset.rollbackTx();
        } catch (Throwable t) {
          LOG.warn("Attempt to rollback partitions failed", t);
        }

        // if this is non-null, then at least some paths have been created. We need to remove them
        if (partitionsToAdd != null) {
          for (String pathToDelete : partitionsToAdd.keySet()) {
            Location locationToDelete = outputDataset.getEmbeddedFileSet().getLocation(pathToDelete);
            try {
              if (locationToDelete.exists()) {
                locationToDelete.delete(true);
              }
            } catch (IOException e) {
              // not sure how this can happen, but we want to keep going. Log and continue
              LOG.warn("Attempt to clean up partition location {} failed", locationToDelete.toURI().getPath(), e);
            }
          }
        }
      } finally {
        // clear this so that we only attempt rollback once in case it gets called multiple times
        outputDataset = null;
        partitionsToAdd = null;
      }
    }
    super.abortJob(context, state);
  }

  // Based off method from FileOutputCommitter: mergePaths(FileSystem fs, final FileStatus from, final Path to);
  // It has been modified to handle the case of two DynamicPartitioner MR creating (while allowing append) the output
  // directory for a Partition. If both find that the partition directory does not exist at the same time, then the
  // second one that does the rename from the 'from' directory to the 'to' directory would have actually resulted
  // in moving the 'from' directory INTO the 'to' directory, since it was created by the first MR.
  /**
   * Merge two paths together.  Anything in from will be moved into to, if there
   * are any name conflicts while merging the files or directories in from win.
   * @param fc the FileContext to use
   * @param from the path data is coming from.
   * @param to the path data is going to.
   * @throws IOException on any error
   */
  private void mergePaths(FileContext fc, final FileStatus from, final Path to) throws IOException {
    if (from.isFile()) {
      if (fc.util().exists(to)) {
        if (!fc.delete(to, true)) {
          throw new IOException("Failed to delete " + to);
        }
      }

      fc.rename(from.getPath(), to);
    } else if (from.isDirectory()) {
      if (!fc.util().exists(to)) {
        //it does not exist just rename
        try {
          fc.rename(from.getPath(), to);
          return;
        } catch (FileAlreadyExistsException e) {
          // This race condition can happen if two MR (DynamicPartitioner) are appending to partitions, and both
          // see the output partition directory as nonexistent, and so both attempt a rename
          // If that happens, simply move the files as below.
        }
      }
      FileStatus toStat = fc.getFileStatus(to);
      if (!toStat.isDirectory()) {
        if (!fc.delete(to, true)) {
          throw new IOException("Failed to delete " + to);
        }
        fc.rename(from.getPath(), to);
      } else {
        //It is a directory so merge everything in the directories
        for (FileStatus subFrom : fc.util().listStatus(from.getPath())) {
          Path subTo = new Path(to, subFrom.getPath().getName());
          mergePaths(fc, subFrom, subTo);
        }
      }
    }
  }

  // copied from superclass
  /**
   * Get a list of all paths where output from committed tasks are stored.
   * @param context the context of the current job
   * @return the list of these Paths/FileStatuses.
   * @throws IOException
   */
  private FileStatus[] getAllCommittedTaskPaths(JobContext context) throws IOException {
    Path jobAttemptPath = getJobAttemptPath(context);
    FileSystem fs = jobAttemptPath.getFileSystem(context.getConfiguration());
    return fs.listStatus(jobAttemptPath, new CommittedTaskFilter());
  }

  /**
   * given two paths as input:
   *    base: /my/base/path
   *    file: /my/base/path/some/other/file
   * return "some/other/file"
   */
  private String getRelative(Path base, Path file) {
    return base.toUri().relativize(file.toUri()).getPath();
  }

  private static class CommittedTaskFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      return !PENDING_DIR_NAME.equals(path.getName());
    }
  }
}
