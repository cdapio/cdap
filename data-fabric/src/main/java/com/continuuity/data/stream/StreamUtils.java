/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Collection of helper methods.
 *
 * TODO: Usage of this class needs to be refactor, as some methods are temporary (e.g. encodeMap/decodeMap).
 */
final class StreamUtils {

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

  static Location getStreamBaseLocation(LocationFactory locationFactory, CConfiguration cConf) {
    return locationFactory.create(cConf.get(Constants.Stream.BASE_DIR));
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
   * @param uri Location to the event file.
   * @return The partition name.
   */
  static String getPartitionName(URI uri) {
    String path = uri.getPath();
    int idx = path.lastIndexOf('/');
    Preconditions.checkArgument(idx >= 0, "Invalid event file location %s.", uri);

    String partitionPath = path.substring(0, idx);
    idx = partitionPath.lastIndexOf('/');
    Preconditions.checkArgument(idx >= 0, "Invalid event file location %s. Failed to determine partition.", uri);
    return partitionPath.substring(idx + 1);
  }

  /**
   * Returns the name of the event bucket based on the file name.
   *
   * @param name Name of the file.
   */
  static String getBucketName(String name) {
    int idx = name.lastIndexOf('.');
    return (idx >= 0) ? name.substring(0, idx) : name;
  }

  /**
   * Gets the partition start time based on the name of the partition.
   *
   * @return The partition start timestamp in milliseconds.
   *
   * @see StreamInputFormat for the naming convention.
   */
  static long getPartitionStartTime(String partitionName) {
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
   * @see StreamInputFormat for the naming convention.
   */
  static long getPartitionEndTime(String partitionName) {
    int idx = partitionName.indexOf('.');
    Preconditions.checkArgument(idx >= 0,
                                "Invalid partition name %s. Partition name should be of format %s",
                                partitionName, "[startTimestamp].[duration]");
    long startTime = Long.parseLong(partitionName.substring(0, idx));
    long duration = Long.parseLong(partitionName.substring(idx + 1));
    return TimeUnit.MILLISECONDS.convert(startTime + duration, TimeUnit.SECONDS);
  }

  private StreamUtils() {
  }
}
