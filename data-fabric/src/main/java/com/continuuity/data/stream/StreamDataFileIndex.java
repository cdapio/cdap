/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.io.BinaryDecoder;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.InputSupplier;
import com.google.common.primitives.Longs;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongListIterator;
import it.unimi.dsi.fastutil.longs.LongLists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * This class is for loading stream index file and lookup of the index.
 */
final class StreamDataFileIndex {

  private static final Logger LOG = LoggerFactory.getLogger(StreamDataFileIndex.class);

  private static final byte[] INDEX_MAGIC_HEADER = {'I', '1'};

  // Parallel array list for holding timestamps and corresponding positions in the index.
  private final LongList timestamps;
  private final LongList positions;

  /**
   * Constructs with the given input.
   *
   * @param indexInputSupplier Provides {@link InputStream} for reading the index.
   */
  StreamDataFileIndex(InputSupplier<? extends InputStream> indexInputSupplier) {
    LongList timestamps;
    LongList positions;

    // Load the whole index into memory.
    try {
      Map.Entry<LongList, LongList> index;
      InputStream indexInput = indexInputSupplier.getInput();
      try {
        index = loadIndex(indexInput);
      } finally {
        Closeables.closeQuietly(indexInput);
      }
      timestamps = LongLists.unmodifiable(index.getKey());
      positions = LongLists.unmodifiable(index.getValue());
    } catch (IOException e) {
      LOG.error("Failed to load stream index. Default to empty index.", e);
      timestamps = LongLists.EMPTY_LIST;
      positions = LongLists.EMPTY_LIST;
    }
    this.timestamps = timestamps;
    this.positions = positions;
  }

  /**
   * Finds the largest event file position recorded in the index that has timestamp smaller than or equal to the given
   * timestamp.
   *
   * @param timestamp Stream event timestamp to search for.
   * @return The file position or {@code -1} if no record satisfied the requirement can be found.
   */
  long floorPositionByTime(long timestamp) {
    if (timestamps.isEmpty()) {
      return -1;
    }

    // Binary search for a timestamp that is larger than or equals to the given timestamp.
    int idx = binarySearch(timestamps, timestamp);
    if (idx >= 0) {
      return positions.getLong(idx);
    }

    // Return the position that has smaller timestamp than the one to search for.
    // If every timestamp in the index is larger than the given one, return -1.
    return idx == -1 ? -1 : positions.getLong(-idx - 2);
  }

  /**
   * Finds the largest event file position recorded in the index that is smaller than or equal to a given offset.
   *
   * @param offset an arbitrary file offset.
   * @return largest event file position that is smaller than or equal to the given offset.
   *
   */
  long floorPosition(long offset) {
    if (positions.isEmpty()) {
      return 0L;
    }

    int idx = binarySearch(positions, offset);
    if (idx >= 0) {
      return offset;
    }

    return idx == -1 ? 0 : positions.getLong(-idx - 2);
  }

  /**
   * Returns a {@link StreamDataFileIndexIterator} for iterating over all (timestamp, position) pairs.
   */
  StreamDataFileIndexIterator indexIterator() {
    final LongListIterator timestampIter = timestamps.iterator();
    final LongListIterator positionIter = positions.iterator();

    return new StreamDataFileIndexIterator() {

      private long timestamp;
      private long position;

      @Override
      public boolean nextIndexEntry() {
        if (timestampIter.hasNext() && positionIter.hasNext()) {
          timestamp = timestampIter.nextLong();
          position = positionIter.nextLong();
          return true;
        }
        return false;
      }

      @Override
      public long currentTimestamp() {
        return timestamp;
      }

      @Override
      public long currentPosition() {
        return position;
      }
    };
  }

  /**
   * Same contract as {@link Collections#binarySearch(java.util.List, Object)}, except that it works on LongList.
   */
  private int binarySearch(LongList list, long target) {
    // Binary search for a value that is larger than or equals to the given target
    int low = 0;
    int high = list.size() - 1;

    while (low <= high) {
      int mid = ((high - low) >> 1) + low;
      long value = list.getLong(mid);

      if (value < target) {
        low = mid + 1;
      } else if (value > target) {
        high = mid - 1;
      } else {
        // Found, returns the index
        return mid;
      }
    }

    return -(low + 1);
  }

  private Map.Entry<LongList, LongList> loadIndex(InputStream input) throws IOException {
    byte[] magic = new byte[INDEX_MAGIC_HEADER.length];
    ByteStreams.readFully(input, magic);

    if (!Arrays.equals(magic, INDEX_MAGIC_HEADER)) {
      throw new IOException("Unsupported index file format. Expected magic bytes as 'I' '1'");
    }

    // Decode the properties map. In current version, it is not used.
    StreamUtils.decodeMap(new BinaryDecoder(input));

    // Read in all index (timestamp, position pairs).
    LongList timestamps = new LongArrayList(1000);
    LongList positions = new LongArrayList(1000);
    byte[] buf = new byte[Longs.BYTES * 2];

    while (ByteStreams.read(input, buf, 0, buf.length) == buf.length) {
      timestamps.add(Bytes.toLong(buf, 0));
      positions.add(Bytes.toLong(buf, Longs.BYTES));
    }

    return Maps.immutableEntry(timestamps, positions);
  }
}
