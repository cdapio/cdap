/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.stream.StreamEventDecoder;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;

/**
 * A {@link InputFormat} for reading stream data. Stream data files are organized by partition directories with
 * bucket files inside.
 *
 * <br/><br/>
 *   Each file has path pattern
 * <pre>
 *     [streamName]/[partitionName]/[bucketName].[dat|idx]
 * OR
 *     [streamName]/[generation]/[partitionName]/[bucketName].[dat|idx]
 * </pre>
 * Where {@code .dat} is the event data file, {@code .idx} is the accompany index file.
 *
 * <br/><br/>
 * The {@code generation} is an integer, representing the stream generation of the data under it. When a stream
 * is truncated, the generation increment by one. The generation {@code 0} is a special case that there is
 * no generation directory.
 *
 * <br/><br/>
 * The {@code partitionName} is formatted as
 * <pre>
 *   [partitionStartTime].[duration]
 * </pre>
 * with both {@code partitionStartTime} and {@code duration} in seconds.
 *
 * <br/><br/>
 * The {@code bucketName} is formatted as
 * <pre>
 *   [prefix].[seqNo]
 * </pre>
 * The {@code seqNo} is a strictly increasing integer for the same prefix starting with 0.
 *
 * @param <K> Key type of input
 * @param <V> Value type of input
 */
public abstract class StreamInputFormat<K, V> extends InputFormat<K, V> {

  private static final String EVENT_START_TIME = "input.streaminputformat.event.starttime";
  private static final String EVENT_END_TIME = "input.streaminputformat.event.endtime";
  private static final String STREAM_PATH = "input.streaminputformat.stream.path";
  private static final String STREAM_TTL = "input.streaminputformat.stream.event.ttl";
  private static final String MAX_SPLIT_SIZE = "input.streaminputformat.max.splits.size";
  private static final String MIN_SPLIT_SIZE = "input.streaminputformat.min.splits.size";

  /**
   * Sets the TTL for the stream events.
   *
   * @param job The job to modify
   * @param ttl TTL of the stream in milliseconds.
   */
  public static void setTTL(Job job, long ttl) {
    Preconditions.checkArgument(ttl >= 0, "TTL must be >= 0");
    job.getConfiguration().setLong(STREAM_TTL, ttl);
  }

  /**
   * Sets the time range for the stream events.
   *
   * @param job The job to modify
   * @param startTime Timestamp in milliseconds of the event start time (inclusive).
   * @param endTime Timestamp in milliseconds of the event end time (exclusive).
   */
  public static void setTimeRange(Job job, long startTime, long endTime) {
    Preconditions.checkArgument(startTime >= 0, "Start time must be >= 0");
    Preconditions.checkArgument(endTime >= 0, "End time must be >= 0");

    job.getConfiguration().setLong(EVENT_START_TIME, startTime);
    job.getConfiguration().setLong(EVENT_END_TIME, endTime);
  }

  /**
   * Sets the base path to stream files.
   *
   * @param job The job to modify.
   * @param path The file path to stream base directory.
   */
  public static void setStreamPath(Job job, URI path) {
    job.getConfiguration().set(STREAM_PATH, path.toString());
  }

  /**
   * Sets the maximum split size.
   *
   * @param job The job to modify.
   * @param maxSplits Maximum split size in bytes.
   */
  public static void setMaxSplitSize(Job job, long maxSplits) {
    job.getConfiguration().setLong(MAX_SPLIT_SIZE, maxSplits);
  }

  /**
   * Sets the minimum split size.
   *
   * @param job The job to modify.
   * @param minSplits Minimum split size in bytes.
   */
  public static void setMinSplitSize(Job job, long minSplits) {
    job.getConfiguration().setLong(MIN_SPLIT_SIZE, minSplits);
  }

  /**
   * Factory method for creating {@link com.continuuity.api.stream.StreamEventDecoder} to decode stream event.
   *
   * @return An instance of {@link com.continuuity.api.stream.StreamEventDecoder}.
   */
  protected abstract StreamEventDecoder<K, V> createStreamEventDecoder();

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    long ttl = conf.getLong(STREAM_TTL, Long.MAX_VALUE);
    long endTime = conf.getLong(EVENT_END_TIME, Long.MAX_VALUE);
    long startTime = Math.max(conf.getLong(EVENT_START_TIME, 0L), getCurrentTime() - ttl);
    Path path = new Path(URI.create(conf.get(STREAM_PATH)));
    long maxSplitSize = conf.getLong(MAX_SPLIT_SIZE, Long.MAX_VALUE);
    long minSplitSize = Math.min(conf.getLong(MIN_SPLIT_SIZE, 1L), maxSplitSize);

    Preconditions.checkArgument(startTime >= 0, "Invalid start time %s", startTime);
    Preconditions.checkArgument(endTime >= 0, "Invalid end time %s", endTime);

    List<InputSplit> splits = Lists.newArrayList();

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
        splitter.computeSplits(fs, minSplitSize, maxSplitSize, startTime, endTime, splits);
      }
    }

    return splits;
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split,
                                               TaskAttemptContext context) throws IOException, InterruptedException {
    return new StreamRecordReader<K, V>(createStreamEventDecoder());
  }

  protected long getCurrentTime() {
    return System.currentTimeMillis();
  }

  /**
   * Collects file status of all buckets under a given partition.
   */
  private Collection<StreamDataFileSplitter> collectBuckets(FileSystem fs, Path partitionPath) throws IOException {
    ImmutableList.Builder<StreamDataFileSplitter> builder = ImmutableList.builder();

    for (FileStatus fileStatus : fs.listStatus(partitionPath)) {
      if (StreamFileType.EVENT.isMatched(fileStatus.getPath().getName())) {
        builder.add(new StreamDataFileSplitter(fileStatus));
      }
    }
    return builder.build();
  }
}
