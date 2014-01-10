/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Helper class for computing {@link InputSplit} for a stream data file.
 *
 * It splits a stream event file into equal size blocks (except the last block). The split size is computed by
 *
 * <br/><br/>
 * {@code Math.min(minSplitSize, Math.max(maxSplitSize, fileBlockSize)) }
 * <br/><br/>
 *
 * Each split produced will also carries {@code startTime} and {@code endTime} so that only stream events within
 * the given time range will get processed.
 */
final class StreamDataFileSplitter {

  private final Map<StreamFileType, FileStatus> fileStatusMap = Maps.newHashMap();

  /**
   * Adds a file status for either an event file or event index file.
   */
  void addFileStatus(FileStatus fileStatus) {
    fileStatusMap.put(StreamFileType.getType(fileStatus.getPath().getName()), fileStatus);
  }

  /**
   * Computes splits for the event file.
   */
  void computeSplits(FileSystem fs, long minSplitSize, long maxSplitSize,
                     long startTime, long endTime, List<InputSplit> splits) throws IOException {
    FileStatus eventFileStatus = fileStatusMap.get(StreamFileType.EVENT);
    FileStatus indexFileStatus = fileStatusMap.get(StreamFileType.INDEX);

    if (eventFileStatus == null || eventFileStatus.getLen() == 0) {
      // Nothing to process.
      return;
    }

    // Compute the splits based on the min/max size
    Path eventFile = eventFileStatus.getPath();
    Path indexFile = indexFileStatus == null ? null : indexFileStatus.getPath();
    BlockLocation[] blockLocations = fs.getFileBlockLocations(eventFile, 0, eventFileStatus.getLen());

    long length = eventFileStatus.getLen();
    long offset = 0;
    int blockIndex = 0;

    while (offset < length) {
      blockIndex = getBlockIndex(blockLocations, offset, blockIndex);
      Preconditions.checkArgument(blockIndex >= 0,
                                  "Failed to find BlockLocation for offset %s in file %s of length %s.",
                                  offset, eventFile, length);

      long splitSize = computeSplitSize(eventFileStatus, offset, minSplitSize, maxSplitSize);
      splits.add(new StreamInputSplit(eventFile, indexFile, startTime, endTime,
                                      offset, splitSize, blockLocations[blockIndex].getHosts()));
      offset += splitSize;
    }
  }

  /**
   * Returns the array index of the given blockLocations that contains the given offset.
   *
   * @param blockLocations Array of {@link BlockLocation} to search for.
   * @param offset File offset.
   * @param startIdx Starting index for the search in the array.
   * @return The array index of the {@link BlockLocation} that contains the given offset.
   */
  private int getBlockIndex(BlockLocation[] blockLocations, long offset, int startIdx) {
    for (int i = startIdx; i < blockLocations.length; i++) {
      BlockLocation blockLocation = blockLocations[i];
      long endOffset = blockLocation.getOffset() + blockLocation.getLength();

      if (blockLocation.getOffset() <= offset && offset < endOffset) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Compute the actual split size. The split size compute would be no larger than the given max split size.
   * The split size would be no smaller than the given min split size, except if number of bytes between
   * offset and file length is smaller than min split size.
   *
   * @param fileStatus The FileStatus of the file to split on.
   * @param offset Starting offset for the split.
   * @param minSplitSize Minimum size for the split.
   * @param maxSplitSize Maximum size for the split.
   * @return
   */
  private long computeSplitSize(FileStatus fileStatus, long offset, long minSplitSize, long maxSplitSize) {
    long blockSize = fileStatus.getBlockSize();
    long splitSize = Math.max(minSplitSize, Math.min(maxSplitSize, blockSize));
    return Math.min(splitSize, fileStatus.getLen() - offset);
  }
}
