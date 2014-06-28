/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.data.stream;

import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;

import java.util.List;

/**
 * This class is for using a Stream as input for a MapReduce job. An instance of this class should be set in the
 * {@code MapReduceContext} of the {@code beforeSubmit} method to use the Stream as input.
 *
 * <pre>
 * {@code
 *
 * class MyMapReduce implements MapReduce {
 *    public void beforeSubmit(MapReduceContext context) {
 *      context.setInput(new StreamBatchReadable("mystream"), null);
 *    }
 * }
 * }
 * </pre>
 *
 */
public class StreamBatchReadable implements BatchReadable<Long, String> {

  private final String streamName;
  private final long startTime;
  private final long endTime;

  /**
   * Constructs an instance with the given stream name with all time range.
   *
   * @param streamName Name of the stream.
   */
  public StreamBatchReadable(String streamName) {
    this(streamName, 0, Long.MAX_VALUE);
  }

  /**
   * Constructs an instance with the given properties.
   *
   * @param streamName Name of the stream.
   * @param startTime Start timestamp in milliseconds.
   * @param endTime End timestamp in milliseconds.
   */
  public StreamBatchReadable(String streamName, long startTime, long endTime) {
    this.streamName = streamName;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  public String getStreamName() {
    return streamName;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  @Override
  public List<Split> getSplits() {
    // Not used.
    return null;
  }

  @Override
  public SplitReader<Long, String> createSplitReader(Split split) {
    // Not used.
    return null;
  }
}
