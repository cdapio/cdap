package com.continuuity.data.operation.ttqueue;

import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.ListIterator;

/**
 * This represents an ordered set of queue entry ids that have been claimed
 * by a queue consumer. It is implemented as a sorted set of ranges. It gets
 * persisted as part of the consumer's state
 */
class ClaimedEntryList implements Comparable<ClaimedEntryList> {

  private List<ClaimedEntryRange> ranges;

  public ClaimedEntryList() {
    this.ranges = Lists.newLinkedList();
  }

  public void add(long begin, long end) {
    ClaimedEntryRange newRange = new ClaimedEntryRange(begin, end);
    if (newRange.isValid()) {
      this.add(newRange);
    }
  }

  public void add(ClaimedEntryRange newRange) {
    ListIterator<ClaimedEntryRange> iterator = ranges.listIterator();
    while (iterator.hasNext()) {
      ClaimedEntryRange current = iterator.next();
      Preconditions.checkArgument(!current.overlapsWith(newRange), "Attempt to add new range %s that overlaps with " +
        "existing claimed entries %s", newRange, this);
      // current element is greater than new range and does not overlap. That means its begin is greater than the
      // end of the new range. Since all following element are even greater, they also don't overlap.
      if (current.compareTo(newRange) > 0) {
        iterator.previous();
        break;
      }
    }
    iterator.add(newRange);
  }

  public void addAll(ClaimedEntryList claimedEntryList) {
    for (ClaimedEntryRange range : claimedEntryList.ranges) {
      this.add(range);
    }
  }

  public void moveForwardTo(long entryId) {
    boolean firstRange = true;
    while (!this.ranges.isEmpty()) {
      ClaimedEntryRange range = this.ranges.get(0);
      Preconditions.checkArgument(!firstRange || range.getBegin() <= entryId,
        "Attempt to move %s backward to entry id %s", this, entryId);
      range.move(entryId);
      if (range.isValid()) {
        break;
      } else {
        ranges.remove(0);
        firstRange = false;
      }
    }
  }

  public ClaimedEntryRange getClaimedEntry() {
    return this.ranges.isEmpty() ? ClaimedEntryRange.INVALID : this.ranges.get(0);
  }

  public long size() {
    long size = 0;
    for (ClaimedEntryRange range : this.ranges) {
      size += range.size();
    }
    return size;
  }

  @Override
  public int compareTo(ClaimedEntryList other) {
    return Long.signum(this.size() - other.size());
  }

  @Override
  public String toString() {
    return this.ranges.toString();
  }

  public void encode(Encoder encoder) throws IOException {
    if (!ranges.isEmpty()) {
      encoder.writeInt(ranges.size());
      for (ClaimedEntryRange range : this.ranges) {
        range.encode(encoder);
      }
    }
    encoder.writeInt(0); // zero denotes end of list as per AVRO spec
  }

  public static ClaimedEntryList decode(Decoder decoder) throws IOException {
    int size = decoder.readInt();
    ClaimedEntryList list = new ClaimedEntryList();
    while (size > 0) {
      for (int i = 0; i < size; ++i) {
        list.add(ClaimedEntryRange.decode(decoder));
      }
      size = decoder.readInt();
    }
    return list;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClaimedEntryList that = (ClaimedEntryList) o;
    return this.ranges.equals(that.ranges);
  }

  @Override
  public int hashCode() {
    return this.ranges.hashCode();
  }
}


