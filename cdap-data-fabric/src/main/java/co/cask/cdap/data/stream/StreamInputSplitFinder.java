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

package co.cask.cdap.data.stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;

/**
 * Finds input splits for a stream given several stream configuration settings and the location of the stream.
 * TODO: support multiple time ranges instead of just a single start and end.
 *
 * @param <T> Type of input split to find. Expected to be either mapred.InputSplit or mapreduce.InputSplit.
 * @see StreamInputFormat for details on stream file layout.
 */
public class StreamInputSplitFinder<T> {
  private final long startTime;
  private final long endTime;
  private final long maxSplitSize;
  private final long minSplitSize;
  private final Path path;
  private final StreamInputSplitFactory<T> splitFactory;

  private StreamInputSplitFinder(URI path, long startTime, long endTime, long maxSplitSize,
                                long minSplitSize, StreamInputSplitFactory<T> splitFactory) {
    Preconditions.checkArgument(startTime >= 0, "Invalid start time %s", startTime);
    Preconditions.checkArgument(endTime >= 0, "Invalid end time %s", endTime);
    this.path = new Path(path);
    this.startTime = startTime;
    this.endTime = endTime;
    this.maxSplitSize = maxSplitSize;
    this.minSplitSize = minSplitSize;
    this.splitFactory = splitFactory;
  }

  /**
   * Get the input splits for a stream.
   *
   * @param conf Configuration of the filesystem the stream resides on.
   * @return List of input splits for the stream.
   * @throws IOException
   * @throws InterruptedException
   */
  public List<T> getSplits(Configuration conf) throws IOException, InterruptedException {
    List<T> splits = Lists.newArrayList();

    // Collects all stream event files timestamp, size and block locations information

    // First grab all directories (partition) that matches with the time range.
    FileSystem fs = path.getFileSystem(conf);
    for (FileStatus partitionStatus : fs.listStatus(path)) {

      // partition should be directory
      if (!partitionStatus.isDirectory()) {
        continue;
      }

      // Match the time range
      long partitionStartTime = StreamUtils.getPartitionStartTime(partitionStatus.getPath().getName());
      long partitionEndTime = StreamUtils.getPartitionEndTime(partitionStatus.getPath().getName());
      if (partitionStartTime > endTime || partitionEndTime <= startTime) {
        continue;
      }

      // Collects all bucket file status in the partition.
      Collection<StreamDataFileSplitter> eventFiles = collectBuckets(fs, partitionStatus.getPath());

      // For each bucket inside the partition directory, compute the splits
      for (StreamDataFileSplitter splitter : eventFiles) {
        splitter.computeSplits(fs, minSplitSize, maxSplitSize, startTime, endTime, splits, splitFactory);
      }
    }

    return splits;
  }

  /**
   * Collects file status of all buckets under a given partition.
   */
  private Collection<StreamDataFileSplitter> collectBuckets(FileSystem fs, Path partitionPath)
    throws IOException {
    ImmutableList.Builder<StreamDataFileSplitter> builder = ImmutableList.builder();

    for (FileStatus fileStatus : fs.listStatus(partitionPath)) {
      if (StreamFileType.EVENT.isMatched(fileStatus.getPath().getName())) {
        builder.add(new StreamDataFileSplitter(fileStatus));
      }
    }
    return builder.build();
  }

  /**
   * Get a builder for creating an input split finder for a stream.
   *
   * @param path path of the stream
   * @return builder to create an input split finder for a stream.
   */
  public static Builder builder(URI path) {
    return new Builder(path);
  }

  /**
   * Builder for creating a split finder.
   */
  public static class Builder {
    private final URI path;
    private Long startTime = 0L;
    private Long endTime = Long.MAX_VALUE;
    private Long minSplitSize = 1L;
    private Long maxSplitSize = Long.MAX_VALUE;

    public Builder(URI path) {
      Preconditions.checkNotNull(path, "Path to the stream must not be null.");
      this.path = path;
    }

    public Builder setEndTime(long endTime) {
      this.endTime = endTime;
      return this;
    }

    public Builder setStartTime(long startTime) {
      this.startTime = startTime;
      return this;
    }

    public Builder setMaxSplitSize(long maxSplitSize) {
      this.maxSplitSize = maxSplitSize;
      return this;
    }

    public Builder setMinSplitSize(long minSplitSize) {
      this.minSplitSize = minSplitSize;
      return this;
    }

    /**
     * Build the input split finder given a factory for creating splits.
     *
     * @param splitFactory
     * @param <T> Type of split to find. Expected to be either mapred.InputSplit or mapreduce.InputSplit.
     * @return
     */
    public <T> StreamInputSplitFinder<T> build(StreamInputSplitFactory<T> splitFactory) {
      return new StreamInputSplitFinder<T>(path, startTime, endTime, maxSplitSize, minSplitSize, splitFactory);
    }
  }
}
