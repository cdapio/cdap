/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.filesystem.Location;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Collection of helper methods.
 *
 * TODO: Usage of this class needs to be refactor, as some methods are temporary (e.g. encodeMap/decodeMap).
 */
public final class StreamUtils {

  /**
   * Decode a map.
   */
  static Map<String, String> decodeMap(Decoder decoder) throws IOException {
    ImmutableMap.Builder<String, String> map = ImmutableMap.builder();
    int len = decoder.readInt();
    while (len != 0) {
      for (int i = 0; i < len; i++) {
        String key = decoder.readString();
        String value = decoder.readInt() == 0 ? decoder.readString() : (String) decoder.readNull();
        map.put(key, value);
      }
      len = decoder.readInt();
    }
    return map.build();
  }

  /**
   * Encodes a map.
   */
  static void encodeMap(Map<String, String> map, Encoder encoder) throws IOException {
    encoder.writeInt(map.size());
    for (Map.Entry<String, String> entry : map.entrySet()) {
      String value = entry.getValue();
      encoder.writeString(entry.getKey())
        .writeInt(value == null ? 1 : 0)
        .writeString(entry.getValue());
    }
    if (!map.isEmpty()) {
      encoder.writeInt(0);
    }
  }

  /**
   * Finds the partition name from the given event file location.
   *
   * @param eventLocation Location to the event file.
   * @return The partition name.
   * @see StreamInputFormat
   */
  public static String getPartitionName(Location eventLocation) {
    URI uri = eventLocation.toURI();
    String path = uri.getPath();
    int endIdx = path.lastIndexOf('/');
    Preconditions.checkArgument(endIdx >= 0,
                                "Invalid event path %s. Partition is missing.", uri);

    int startIdx = path.lastIndexOf('/', endIdx - 1);
    Preconditions.checkArgument(startIdx < endIdx,
                                "Invalid event path %s. Partition is missing.", uri);

    return path.substring(startIdx + 1, endIdx);
  }

  /**
   * Returns the name of the event bucket based on the file name.
   *
   * @param name Name of the file.
   * @see StreamInputFormat
   */
  public static String getBucketName(String name) {
    // Strip off the file extension
    int idx = name.lastIndexOf('.');
    return (idx >= 0) ? name.substring(0, idx) : name;
  }

  /**
   * Returns the file prefix based on the given file name.
   *
   * @param name Name of the file.
   * @return The prefix part of the stream file.
   * @see StreamInputFormat
   */
  public static String getNamePrefix(String name) {
    String bucketName = getBucketName(name);
    int idx = bucketName.lastIndexOf('.');
    Preconditions.checkArgument(idx >= 0, "Invalid name %s. Name is expected in [prefix].[seqId] format", bucketName);
    return bucketName.substring(0, idx);
  }

  /**
   * Returns the sequence number of the given file name.
   *
   * @param name Name of the file.
   * @return The sequence number of the stream file.
   * @see StreamInputFormat
   */
  public static int getSequenceId(String name) {
    String bucketName = getBucketName(name);
    int idx = bucketName.lastIndexOf('.');
    Preconditions.checkArgument(idx >= 0 && (idx + 1) < bucketName.length(),
                                "Invalid name %s. Name is expected in [prefix].[seqId] format", bucketName);
    return Integer.parseInt(bucketName.substring(idx + 1));
  }

  /**
   * Gets the partition start time based on the name of the partition.
   *
   * @return The partition start timestamp in milliseconds.
   *
   * @see StreamInputFormat
   */
  public static long getPartitionStartTime(String partitionName) {
    int idx = partitionName.indexOf('.');
    Preconditions.checkArgument(idx >= 0,
                                "Invalid partition name %s. Partition name should be of format %s",
                                partitionName, "[startTimestamp].[duration]");
    return TimeUnit.MILLISECONDS.convert(Long.parseLong(partitionName.substring(0, idx)), TimeUnit.SECONDS);
  }

  /**
   * Gets the partition end time based on the name of the partition.
   *
   * @return the partition end timestamp in milliseconds.
   *
   * @see StreamInputFormat
   */
  public static long getPartitionEndTime(String partitionName) {
    int idx = partitionName.indexOf('.');
    Preconditions.checkArgument(idx >= 0,
                                "Invalid partition name %s. Partition name should be of format %s",
                                partitionName, "[startTimestamp].[duration]");
    long startTime = Long.parseLong(partitionName.substring(0, idx));
    long duration = Long.parseLong(partitionName.substring(idx + 1));
    return TimeUnit.MILLISECONDS.convert(startTime + duration, TimeUnit.SECONDS);
  }

  /**
   * Creates stream base location with the given generation.
   *
   * @param streamBaseLocation the base directory for the stream
   * @param generation generation id
   * @return Location for the given generation
   *
   * @see StreamInputFormat
   */
  public static Location createGenerationLocation(Location streamBaseLocation, int generation) throws IOException {
    // 0 padding generation is just for sorted view in ls. Not carry any special meaning.
    return (generation == 0) ? streamBaseLocation : streamBaseLocation.append(String.format("%06d", generation));
  }

  /**
   * Creates the location for the partition directory.
   *
   * @param baseLocation Base location for partition directory.
   * @param partitionStart Partition start timestamp in milliseconds.
   * @param partitionDuration Partition duration in milliseconds.
   * @return The location for the partition directory.
   */
  public static Location createPartitionLocation(Location baseLocation,
                                                 long partitionStart, long partitionDuration) throws IOException {
    // 0 padding is just for sorted view in ls. Not carry any special meaning.
    String path = String.format("%010d.%05d",
                                TimeUnit.SECONDS.convert(partitionStart, TimeUnit.MILLISECONDS),
                                TimeUnit.SECONDS.convert(partitionDuration, TimeUnit.MILLISECONDS));

    return baseLocation.append(path);
  }

  /**
   * Creates location for stream file.
   *
   * @param partitionLocation The partition directory location.
   * @param prefix File prefix.
   * @param seqId Sequence number of the file.
   * @param type Type of the stream file.
   * @return The location of the stream file.
   *
   * @see StreamInputFormat for naming convention.
   */
  public static Location createStreamLocation(Location partitionLocation, String prefix,
                                              int seqId, StreamFileType type) throws IOException {
    // 0 padding sequence id is just for sorted view in ls. Not carry any special meaning.
    return partitionLocation.append(String.format("%s.%06d.%s", prefix, seqId, type.getSuffix()));
  }

  /**
   * Returns the aligned partition start time.
   *
   * @param timestamp Timestamp in milliseconds.
   * @param partitionDuration Partition duration in milliseconds.
   * @return The partition start time of the given timestamp.
   */
  public static long getPartitionStartTime(long timestamp, long partitionDuration) {
    return timestamp / partitionDuration * partitionDuration;
  }

  /**
   * Encode a {@link StreamFileOffset} instance.
   *
   * @param out Output for encoding
   * @param offset The offset object to encode
   */
  public static void encodeOffset(DataOutput out, StreamFileOffset offset) throws IOException {
    out.writeInt(offset.getGeneration());
    out.writeLong(offset.getPartitionStart());
    out.writeLong(offset.getPartitionEnd());
    out.writeUTF(offset.getNamePrefix());
    out.writeInt(offset.getSequenceId());
    out.writeLong(offset.getOffset());
  }

  /**
   * Decode a {@link StreamFileOffset} encoded by the {@link #encodeOffset(DataOutput, StreamFileOffset)}
   * method.
   *
   * @param config Stream configuration for the stream that the offset is representing
   * @param in Input for decoding
   * @return A new instance of {@link StreamFileOffset}
   */
  public static StreamFileOffset decodeOffset(StreamConfig config, DataInput in) throws IOException {
    int generation = in.readInt();
    long partitionStart = in.readLong();
    long duration = in.readLong() - partitionStart;
    String prefix = in.readUTF();
    int seqId = in.readInt();
    long offset = in.readLong();

    Location baseLocation = config.getLocation();
    if (generation > 0) {
      baseLocation = createGenerationLocation(baseLocation, generation);
    }
    Location partitionLocation = createPartitionLocation(baseLocation, partitionStart, duration);
    Location eventLocation = createStreamLocation(partitionLocation, prefix, seqId, StreamFileType.EVENT);
    return new StreamFileOffset(eventLocation, offset, generation);
  }

  public static StreamConfig ensureExists(StreamAdmin admin, String streamName) throws IOException {
    try {
      return admin.getConfig(streamName);
    } catch (Exception e) {
      // Ignored
    }
    try {
      admin.create(streamName);
      return admin.getConfig(streamName);
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw new IOException(e);
    }
  }

  /**
   * Finds the current generation id of a stream. It scans the stream directory to look for largest generation
   * number in directory name.
   *
   * @param config configuration of the stream
   * @return the generation id
   */
  public static int getGeneration(StreamConfig config) throws IOException {
    Location streamLocation = config.getLocation();

    // Default generation is 0.
    int genId = 0;
    CharMatcher numMatcher = CharMatcher.inRange('0', '9');

    List<Location> locations = streamLocation.list();
    if (locations == null) {
      return 0;
    }

    for (Location location : locations) {
      if (numMatcher.matchesAllOf(location.getName()) && location.isDirectory()) {
        int id = Integer.parseInt(location.getName());
        if (id > genId) {
          genId = id;
        }
      }
    }
    return genId;
  }

  private StreamUtils() {
  }
}
