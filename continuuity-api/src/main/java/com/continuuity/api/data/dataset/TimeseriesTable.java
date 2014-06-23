/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data.dataset;

import java.util.List;

/**
 * Defines simple timeseries dataset.
 *
 * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.dataset.lib.TimeseriesTable}
 */
@Deprecated
public interface TimeseriesTable {

  /**
   * Stores entry in dataset.
   * See {@link Entry} for more details.
   * @param entry to store.
   */
  void write(Entry entry);

  /**
   * Reads entries of a time range with given key and tags set.
   *
   * @param key key of the entries to read
   * @param startTime defines start of the time range to read, inclusive.
   * @param endTime defines end of the time range to read, inclusive.
   * @param tags defines a set of tags ALL of which MUST present in every returned entry. Can be absent, in that case
   *             no filtering by tags will be applied.
   *        NOTE: return entries contain all tags that were providing during writing, NOT passed with this param.
   *
   * @return list of entries that satisfy provided conditions.
   *
   * @throws IllegalArgumentException when provided condition is incorrect.
   */
  List<Entry> read(byte key[], long startTime, long endTime, byte[]... tags);

  /**
   * Timeseries dataset entry.
   */
  public static final class Entry {
    private byte[] key;
    private byte[] value;
    private long timestamp;
    private byte[][] tags;

    /**
     * Creates instance of the timeseries entry.
     * @param key key of the entry. E.g. "metric1"
     * @param value value to store
     * @param timestamp timestamp of the entry
     * @param tags optional list of tags associated with the entry
     */
    public Entry(final byte[] key, final byte[] value, final long timestamp, final byte[]... tags) {
      this.key = key;
      this.value = value;
      this.timestamp = timestamp;
      this.tags = tags;
    }

    public byte[] getKey() {
      return key;
    }

    public byte[] getValue() {
      return value;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public byte[][] getTags() {
      return tags;
    }
  }
}
