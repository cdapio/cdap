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
package co.cask.cdap.hive.stream;

import com.google.common.base.Objects;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Represents a mapred InputSplit for stream. Any InputSplit used by Hive MUST be a FileInputSplit otherwise
 * Hive will choke and die. Hive will pass this into HiveInputSplit, which happily returns an invalid path if this is
 * not a FileSplit. The invalid path causes mapred to die, but not in a way where any sensible error message is logged.
 * Luckily for us stream splits are file splits.
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
