/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.net.URI;
import java.util.List;

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

  private final FileStatus eventFileStatus;

  StreamDataFileSplitter(FileStatus eventFileStatus) {
    this.eventFileStatus = eventFileStatus;
  }

  /**
   * Computes splits for the event file.
   */
  void computeSplits(FileSystem fs, long minSplitSize, long maxSplitSize,
                     long startTime, long endTime, List<InputSplit> splits) throws IOException {

    // Compute the splits based on the min/max size
    Path eventFile = eventFileStatus.getPath();
    Path indexFile = getIndexFile(eventFile);

    BlockLocation[] blockLocations = fs.getFileBlockLocations(eventFile, 0, eventFileStatus.getLen());

    long length = eventFileStatus.getLen();
    long offset = 0;
    int blockIndex = 0;

    while (offset < length) {
      blockIndex = getBlockIndex(blockLocations, offset, blockIndex);
      String[] hosts = null;
      if (blockIndex >= 0) {
        hosts = blockLocations[blockIndex].getHosts();
      } else {
        blockIndex = 0;
      }

      long splitSize = computeSplitSize(eventFileStatus, offset, minSplitSize, maxSplitSize);
      splits.add(new StreamInputSplit(eventFile, indexFile, startTime, endTime, offset, splitSize, hosts));
      offset += splitSize;
    }

    // One extra split for the tail of the file.
    splits.add(new StreamInputSplit(eventFile, indexFile, startTime, endTime, offset, Long.MAX_VALUE, null));
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
    if (blockLocations == null) {
      return -1;
    }
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

  private Path getIndexFile(Path eventFile) {
    String eventPath = eventFile.toUri().toString();
    int extLength = StreamFileType.EVENT.getSuffix().length();
    return new Path(URI.create(String.format("%s%s",
                                             eventPath.substring(0, eventPath.length() - extLength),
                                             StreamFileType.INDEX.getSuffix())));
  }
}
