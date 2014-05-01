/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.google.common.base.Objects;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Represents an InputSplit for stream.
 */
public final class StreamInputSplit extends FileSplit implements Writable {

  private Path indexPath;
  private long startTime;
  private long endTime;

  /**
   * Default constructor. Called by MapReduce framework only.
   */
  public StreamInputSplit() {
    // No-op
  }

  /**
   * Constructs a split.
   *
   * @param path Path for the stream event file.
   * @param indexPath Path for the stream index file.
   * @param startTime Event start timestamp in milliseconds (inclusive).
   * @param endTime Event end timestamp in milliseconds (exclusive).
   * @param start Starting offset in the stream event file. It can be arbitrary offset, no need to align to event start.
   * @param length Size of this split.
   * @param locations List of hosts containing this split.
   */
  StreamInputSplit(Path path, Path indexPath, long startTime, long endTime,
                   long start, long length, @Nullable String[] locations) {
    super(path, start, length, locations);
    this.indexPath = indexPath;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  /**
   * Returns the path for index file.
   */
  public Path getIndexPath() {
    return indexPath;
  }

  /**
   * Returns the event start timestamp in milliseconds.
   */
  long getStartTime() {
    return startTime;
  }

  /**
   * Returns the event end timestamp in milliseconds.
   */
  long getEndTime() {
    return endTime;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    if (indexPath == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      WritableUtils.writeString(out, indexPath.toString());
    }
    WritableUtils.writeVLong(out, startTime);
    WritableUtils.writeVLong(out, endTime);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    boolean hasIndex = in.readBoolean();
    if (hasIndex) {
      indexPath = new Path(WritableUtils.readString(in));
    }
    startTime = WritableUtils.readVLong(in);
    endTime = WritableUtils.readVLong(in);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("path", getPath())
      .add("index", getIndexPath())
      .add("start", getStart())
      .add("length", getLength())
      .add("startTime", getStartTime())
      .add("endTime", getEndTime())
      .toString();
  }
}
