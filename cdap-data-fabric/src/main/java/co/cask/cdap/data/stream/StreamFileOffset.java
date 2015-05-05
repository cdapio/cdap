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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.net.URI;

/**
 * Represents offset information for single stream file. The comparison of offset instance
 * is done by comparing event location path followed by offset.
 */
public final class StreamFileOffset {

  private final Location eventLocation;
  private final Location indexLocation;
  private final long partitionStart;
  private final long partitionEnd;
  private final String namePrefix;
  private final int seqId;
  private final long offset;
  private final int generation;

  /**
   * Clones from another {@link StreamFileOffset}.
   * @param other The instance to clone from.
   */
  public StreamFileOffset(StreamFileOffset other) {
    this(other, other.getOffset());
  }

  /**
   * Clones from another {@link StreamFileOffset} but with a different offset value.
   *
   * @param other The instance to clone from
   * @param offset file offset
   */
  public StreamFileOffset(StreamFileOffset other, long offset) {
    this.eventLocation = other.eventLocation;
    this.indexLocation = other.indexLocation;
    this.partitionStart = other.partitionStart;
    this.partitionEnd = other.partitionEnd;
    this.namePrefix = other.namePrefix;
    this.seqId = other.seqId;
    this.generation = other.generation;
    this.offset = offset;
  }

  public StreamFileOffset(Location eventLocation, long offset, int generation) {
    Preconditions.checkNotNull(eventLocation, "Event file location cannot be null.");

    this.eventLocation = eventLocation;
    this.indexLocation = createIndexLocation(eventLocation);
    this.offset = offset;

    // See StreamInputFormat for the path format
    String partitionName = StreamUtils.getPartitionName(eventLocation);
    this.partitionStart = StreamUtils.getPartitionStartTime(partitionName);
    this.partitionEnd = StreamUtils.getPartitionEndTime(partitionName);

    this.namePrefix = StreamUtils.getNamePrefix(eventLocation.getName());
    this.seqId = StreamUtils.getSequenceId(eventLocation.getName());

    this.generation = generation;
  }

  public Location getEventLocation() {
    return eventLocation;
  }

  public Location getIndexLocation() {
    return indexLocation;
  }

  public long getOffset() {
    return offset;
  }

  public long getPartitionStart() {
    return partitionStart;
  }

  public long getPartitionEnd() {
    return partitionEnd;
  }

  public String getNamePrefix() {
    return namePrefix;
  }

  public int getSequenceId() {
    return seqId;
  }

  public int getGeneration() {
    return generation;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("event", eventLocation.toURI())
      .add("offset", getOffset())
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StreamFileOffset other = (StreamFileOffset) o;

    return (offset == other.offset) && Objects.equal(eventLocation, other.eventLocation);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(eventLocation, offset);
  }

  /**
   * Creates the index file location from the event file location.
   *
   * @param eventLocation Location for the event file.
   * @return Location of the index file.
   */
  private Location createIndexLocation(Location eventLocation) {
    LocationFactory factory = eventLocation.getLocationFactory();
    String eventPath = eventLocation.toURI().toString();
    int extLength = StreamFileType.EVENT.getSuffix().length();

    return factory.create(URI.create(String.format("%s%s",
                                                   eventPath.substring(0, eventPath.length() - extLength),
                                                   StreamFileType.INDEX.getSuffix())));
  }
}
