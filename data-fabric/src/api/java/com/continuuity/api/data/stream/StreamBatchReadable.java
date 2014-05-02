/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.data.stream;

import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;

import java.util.List;

/**
 * This class is a temporary hack for setting stream dynamically for using in MapReduce.
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
