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

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.stream.StreamEventDecoder;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Stream mapreduce input format. Stream data files are organized by partition directories with bucket files inside.
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
public class StreamInputFormat<K, V> extends InputFormat<K, V> {
  private static final StreamInputSplitFactory<InputSplit> splitFactory = new StreamInputSplitFactory<InputSplit>() {
    @Override
    public InputSplit createSplit(Path path, Path indexPath, long startTime, long endTime,
                                  long start, long length, @Nullable String[] locations) {
      return new StreamInputSplit(path, indexPath, startTime, endTime, start, length, locations);
    }
  };

  private static final String EVENT_START_TIME = "input.streaminputformat.event.starttime";
  private static final String EVENT_END_TIME = "input.streaminputformat.event.endtime";
  private static final String STREAM_PATH = "input.streaminputformat.stream.path";
  private static final String STREAM_TTL = "input.streaminputformat.stream.event.ttl";
  private static final String MAX_SPLIT_SIZE = "input.streaminputformat.max.splits.size";
  private static final String MIN_SPLIT_SIZE = "input.streaminputformat.min.splits.size";
  private static final String DECODER_TYPE = "input.streaminputformat.decoder.type";

  /**
   * Sets the TTL for the stream events.
   *
   * @param job  The job to modify
   * @param ttl TTL of the stream in milliseconds.
   */
  public static void setTTL(Job job, long ttl) {
    setTTL(job.getConfiguration(), ttl);
  }

  /**
   * Sets the TTL for the stream events.
   *
   * @param conf  The configuration to modify
   * @param ttl TTL of the stream in milliseconds.
   */
  public static void setTTL(Configuration conf, long ttl) {
    Preconditions.checkArgument(ttl >= 0, "TTL must be >= 0");
    conf.setLong(STREAM_TTL, ttl);
  }

  /**
   * Sets the time range for the stream events.
   *
   * @param job The job to modify
   * @param startTime Timestamp in milliseconds of the event start time (inclusive).
   * @param endTime Timestamp in milliseconds of the event end time (exclusive).
   */
  public static void setTimeRange(Job job, long startTime, long endTime) {
    setTimeRange(job.getConfiguration(), startTime, endTime);
  }

  /**
   * Sets the time range for the stream events.
   *
   * @param conf The configuration to modify
   * @param startTime Timestamp in milliseconds of the event start time (inclusive).
   * @param endTime Timestamp in milliseconds of the event end time (exclusive).
   */
  public static void setTimeRange(Configuration conf, long startTime, long endTime) {
    Preconditions.checkArgument(startTime >= 0, "Start time must be >= 0");
    Preconditions.checkArgument(endTime >= 0, "End time must be >= 0");

    conf.setLong(EVENT_START_TIME, startTime);
    conf.setLong(EVENT_END_TIME, endTime);
  }

  /**
   * Sets the base path to stream files.
   *
   * @param job The job to modify.
   * @param path The file path to stream base directory.
   */
  public static void setStreamPath(Job job, URI path) {
    setStreamPath(job.getConfiguration(), path);
  }

  /**
   * Sets the base path to stream files.
   *
   * @param conf The conf to modify.
   * @param path The file path to stream base directory.
   */
  public static void setStreamPath(Configuration conf, URI path) {
    conf.set(STREAM_PATH, path.toString());
  }

  /**
   * Sets the maximum split size.
   *
   * @param job The job to modify.
   * @param maxSplits Maximum split size in bytes.
   */
  public static void setMaxSplitSize(Job job, long maxSplits) {
    setMaxSplitSize(job.getConfiguration(), maxSplits);
  }

  /**
   * Sets the maximum split size.
   *
   * @param conf The conf to modify.
   * @param maxSplits Maximum split size in bytes.
   */
  public static void setMaxSplitSize(Configuration conf, long maxSplits) {
    conf.setLong(MAX_SPLIT_SIZE, maxSplits);
  }

  /**
   * Sets the minimum split size.
   *
   * @param job The job to modify.
   * @param minSplits Minimum split size in bytes.
   */
  public static void setMinSplitSize(Job job, long minSplits) {
    setMinSplitSize(job.getConfiguration(), minSplits);
  }

  /**
   * Sets the minimum split size.
   *
   * @param conf The conf to modify.
   * @param minSplits Minimum split size in bytes.
   */
  public static void setMinSplitSize(Configuration conf, long minSplits) {
    conf.setLong(MIN_SPLIT_SIZE, minSplits);
  }

  /**
   * Sets the class name for the {@link StreamEventDecoder}.
   *
   * @param job The job to modify.
   * @param decoderClassName Class name of the decoder class
   */
  public static void setDecoderClassName(Job job, String decoderClassName) {
    setDecoderClassName(job.getConfiguration(), decoderClassName);
  }

  /**
   * Sets the class name for the {@link StreamEventDecoder}.
   *
   * @param conf The conf to modify.
   * @param decoderClassName Class name of the decoder class
   */
  public static void setDecoderClassName(Configuration conf, String decoderClassName) {
    conf.set(DECODER_TYPE, decoderClassName);
  }

  /**
   * Returns the {@link StreamEventDecoder} class as specified in the job configuration.
   *
   * @param conf The job configuration
   * @return The {@link StreamEventDecoder} class or {@code null} if it is not set.
   */
  public static Class<? extends StreamEventDecoder> getDecoderClass(Configuration conf) {
    return conf.getClass(DECODER_TYPE, null, StreamEventDecoder.class);
  }

  /**
   * Tries to set the {@link StreamInputFormat#DECODER_TYPE} depending upon the supplied value class
   *
   * @param conf   the conf to modify
   * @param vClass the value class Type
   */
  public static void inferDecoderClass(Configuration conf, Type vClass) {
    if (Text.class.equals(vClass)) {
      setDecoderClassName(conf, TextStreamEventDecoder.class.getName());
    } else if (BytesWritable.class.equals(vClass)) {
      setDecoderClassName(conf, BytesStreamEventDecoder.class.getName());
    } else if (vClass instanceof Class && ((Class) vClass).isAssignableFrom(StreamEvent.class)) {
      setDecoderClassName(conf, IdentityStreamEventDecoder.class.getName());
    } else {
      throw new IllegalArgumentException("The value class must be of type BytesWritable, Text, StreamEvent or " +
                                           "StreamEventData if no decoder type is provided");
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    long ttl = conf.getLong(STREAM_TTL, Long.MAX_VALUE);
    long endTime = conf.getLong(EVENT_END_TIME, Long.MAX_VALUE);
    long startTime = Math.max(conf.getLong(EVENT_START_TIME, 0L), getCurrentTime() - ttl);
    long maxSplitSize = conf.getLong(MAX_SPLIT_SIZE, Long.MAX_VALUE);
    long minSplitSize = Math.min(conf.getLong(MIN_SPLIT_SIZE, 1L), maxSplitSize);
    StreamInputSplitFinder<InputSplit> splitFinder = StreamInputSplitFinder.builder(URI.create(conf.get(STREAM_PATH)))
      .setStartTime(startTime)
      .setEndTime(endTime)
      .setMinSplitSize(minSplitSize)
      .setMaxSplitSize(maxSplitSize)
      .build(splitFactory);
    return splitFinder.getSplits(conf);
  }

  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split,
                                               TaskAttemptContext context) throws IOException, InterruptedException {
    return new StreamRecordReader<K, V>(createStreamEventDecoder(context.getConfiguration()));
  }

  protected long getCurrentTime() {
    return System.currentTimeMillis();
  }

  @SuppressWarnings("unchecked")
  protected StreamEventDecoder<K, V> createStreamEventDecoder(Configuration conf) {
    Class<? extends StreamEventDecoder> decoderClass = getDecoderClass(conf);
    Preconditions.checkNotNull(decoderClass, "Failed to load stream event decoder %s", conf.get(DECODER_TYPE));
    try {
      return (StreamEventDecoder<K, V>) decoderClass.newInstance();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
