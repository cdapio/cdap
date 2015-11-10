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

import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.batch.BasicMapReduceTaskContext;
import co.cask.cdap.internal.app.runtime.batch.MapReduceClassLoader;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    PartitionedFileSet outputDataset = taskContext.getDataset(outputDatasetName);
    Partitioning partitioning = outputDataset.getPartitioning();

    Set<PartitionKey> partitionsToAdd = new HashSet<>();
    Set<String> relativePaths = new HashSet<>();
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
        String fileName = relativePath.substring(lastPathSepIdx + 1);

        Path finalDir = new Path(FileOutputFormat.getOutputPath(context), relativeDir);
        Path finalPath = new Path(finalDir, fileName);
        if (fs.exists(finalPath)) {
          throw new FileAlreadyExistsException("Final output path " + finalPath + " already exists");
        }
        PartitionKey partitionKey = getPartitionKey(partitioning, relativeDir);
        partitionsToAdd.add(partitionKey);
        relativePaths.add(relativeDir);
      }
    }

    // We need to copy to the parent of the FileOutputFormat's outputDir, since we added a _temporary_jobId suffix to
    // the original outputDir.
    Path finalOutput = FileOutputFormat.getOutputPath(context);
    FileSystem fs = finalOutput.getFileSystem(configuration);
    for (FileStatus stat : getAllCommittedTaskPaths(context)) {
      mergePaths(fs, stat, finalOutput);
    }

    // create all the necessary partitions
    for (PartitionKey partitionKey : partitionsToAdd) {
      outputDataset.getPartitionOutput(partitionKey).addPartition();
    }

    // close the TaskContext, which flushes dataset operations
    try {
      taskContext.flushOperations();
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
      throw new IOException(e);
    }

    // delete the job-specific _temporary folder and create a _done file in the o/p folder
    cleanupJob(context);

    // mark all the final output paths with a _SUCCESS file, if configured to do so (default = true)
    if (configuration.getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)) {
      for (String relativePath : relativePaths) {
        Path pathToMark = new Path(finalOutput, relativePath);
        Path markerPath = new Path(pathToMark, SUCCEEDED_FILE_NAME);
        fs.createNewFile(markerPath);
      }
    }
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

  // Copied from superclass to enable usage of it, because our 'from' and 'to' locations are different.
  /**
   * Merge two paths together.  Anything in from will be moved into to, if there
   * are any name conflicts while merging the files or directories in from win.
   * @param fs the File System to use
   * @param from the path data is coming from.
   * @param to the path data is going to.
   * @throws IOException on any error
   */
  private void mergePaths(FileSystem fs, final FileStatus from, final Path to) throws IOException {
    if (from.isFile()) {
      if (fs.exists(to)) {
        if (!fs.delete(to, true)) {
          throw new IOException("Failed to delete " + to);
        }
      }

      if (!fs.rename(from.getPath(), to)) {
        throw new IOException("Failed to rename " + from + " to " + to);
      }
    } else if (from.isDirectory()) {
      if (fs.exists(to)) {
        FileStatus toStat = fs.getFileStatus(to);
        if (!toStat.isDirectory()) {
          if (!fs.delete(to, true)) {
            throw new IOException("Failed to delete " + to);
          }
          if (!fs.rename(from.getPath(), to)) {
            throw new IOException("Failed to rename " + from + " to " + to);
          }
        } else {
          //It is a directory so merge everything in the directories
          for (FileStatus subFrom: fs.listStatus(from.getPath())) {
            Path subTo = new Path(to, subFrom.getPath().getName());
            mergePaths(fs, subFrom, subTo);
          }
        }
      } else {
        //it does not exist just rename
        if (!fs.rename(from.getPath(), to)) {
          throw new IOException("Failed to rename " + from + " to " + to);
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
