package com.continuuity.data.operation.ttqueue;

import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.data.operation.ttqueue.internal.TTQueueNewConstants;
import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 * This represents a range of queue entry ids that have been claimed by a queue consumer.
 * Multiple of these can be persisted with the consumer state.
 */
public final class ClaimedEntryRange implements Comparable<ClaimedEntryRange> {

  public static final long INVALID_ENTRY_ID = TTQueueNewConstants.INVALID_ENTRY_ID;

  private long begin;
  private long end;

  static final ClaimedEntryRange INVALID = new ClaimedEntryRange(INVALID_ENTRY_ID, INVALID_ENTRY_ID);

  /**
   * Constructor from begin and end of the range.
   * @param begin the begin of the range.
   * @param end the end of the range, must greater or equal to begin
   */
  public ClaimedEntryRange(long begin, long end) {
    // range must begin at or before end
    Preconditions.checkArgument(end >= begin, "begin (%d) is greater than end (%d)", begin, end);
    // begin and end can be INVALID_ENTRY_ID only of they both are
    Preconditions.checkArgument(begin != INVALID_ENTRY_ID && end != INVALID_ENTRY_ID || begin == end,
      "Either begin (%d) or end (%d) is invalid", begin, end);
    this.begin = begin;
    this.end = end;
  }

  public long getBegin() {
    return begin;
  }

  public long getEnd() {
    return end;
  }

  private void invalidate() {
    this.begin = INVALID_ENTRY_ID;
  }

  /**
   * Moves the begin of this range to the given entry id
   * @param entryId the begin entry id for the new range
   */
  public void move(long entryId) {
    if (isValid()) {
      if (entryId > end) {
        this.invalidate();
      } else if (entryId > begin) {
        this.begin = entryId;
      }
    }
  }

  public boolean isValid() {
    return begin != INVALID_ENTRY_ID;
  }

  @Override
  public String toString() {
    return begin + "-" + end;
  }

  public void encode(Encoder encoder) throws IOException {
    encoder.writeLong(begin);
    encoder.writeLong(end);
  }

  public static ClaimedEntryRange decode(Decoder decoder) throws IOException {
    return new ClaimedEntryRange(decoder.readLong(), decoder.readLong());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClaimedEntryRange that = (ClaimedEntryRange) o;
    return (begin == that.begin) && (end == that.end);
  }

  @Override
  public int hashCode() {
    int result = (int) (begin ^ (begin >>> 32));
    result = 31 * result + (int) (end ^ (end >>> 32));
    return result;
  }

  public boolean overlapsWith(ClaimedEntryRange other) {
    // if this begin is within the other range: true
    // if this begin is before the other range: true iff this does not end before the begin of the other
    return this.begin <= other.end && this.end >= other.begin;
  }

  @Override
  public int compareTo(ClaimedEntryRange other) {
    return this.begin < other.begin ? -1 :
      this.begin > other.begin ? 1 :
        this.end < other.end ? -1 :
          this.end > other.end ? 1 :
            0;
  }

  public long size() {
    return this.isValid() ? this.end - this.begin + 1 : 0;
  }
}