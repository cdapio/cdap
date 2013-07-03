package com.continuuity.metrics2.temporaldb.internal;
import com.continuuity.metrics2.temporaldb.DataPoint;
import com.continuuity.metrics2.temporaldb.Query;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;


/**
 * Created with IntelliJ IDEA.
 * User: nmotgi
 * Date: 10/7/12
 * Time: 5:45 PM
 * To change this template use File | Settings | File Templates.
 */
class DataPointSerializer {
  private final UniqueId metrics;
  private final UniqueId tags;
  private final UniqueId tagvalues;

  public DataPointSerializer(final UniqueId metrics, final UniqueId tags,
                             final UniqueId tagvalues) {
    this.metrics = metrics;
    this.tags = tags;
    this.tagvalues = tagvalues;

  }

  public Entry<byte[], byte[]> convert(DataPoint dataPoint) throws Exception {
    String metricName = dataPoint.getMetric();
    if (metricName == null || metricName.isEmpty()) {
      throw new IllegalArgumentException(
        "Metric name cannot not be empty");
    }
    int metricID = metrics.getOrCreateId(metricName);
    long timestamp = dataPoint.getTimestamp();
    int[][] properties = convertTags(dataPoint.getTags());
    double value = dataPoint.getValue();
    byte[] key = createkey(metricID, timestamp, properties);
    byte[] valueBytes = Bytes.fromDouble(value);

    return new SimpleImmutableEntry<byte[], byte[]>(key, valueBytes);
  }

  /**
   * metricID|timeStamp|tagID1|tagValueID1|tagID2|tagValueID2
   *
   * @param metricID
   * @param timestamp
   * @param tags
   * @return
   */
  public byte[] createkey(int metricID, long timestamp, int[][] tags) {
    int propSize = tags == null ? 0 : tags.length;
    int size = 4 + 8 + ((4 + 4) * propSize);
    ByteBuffer bb = ByteBuffer.allocate(size);
    bb.putInt(metricID);
    bb.putLong(timestamp);

    if (tags != null) {
      for (int[] property : tags) {
        bb.putInt(property[0]);
        bb.putInt(property[1]);
      }
    }
    return bb.array();
  }

  public int[][] convertTags(Map<String, String> tags)
    throws Exception {
    if (tags == null || tags.isEmpty()) {
      return new int[0][];
    } else {
      int[][] result = new int[tags.size()][];

      int i = 0;
      for (Entry<String, String> entry : tags.entrySet()) {
        int id = this.tags.getOrCreateId(entry.getKey());
        String value = entry.getValue();
        int valueInt;
        if (value != null && value.equals(Query.WILDCARD)) {
          valueInt = Query.WILDCARD_ID;
        } else {
          valueInt = tagvalues.getOrCreateId(value);
        }
        result[i] = new int[] { id, valueInt };
        i++;
      }
      return result;
    }
  }

  public static int parseMetricID(byte[] key) {
    if (key == null || key.length < 4) {
      return -1;
    } else {
      ByteBuffer bb = ByteBuffer.wrap(key);
      return bb.getInt(); // metric id
    }
  }

  public static long parseTimeStamp(byte[] key) {
    if (key == null || key.length < 12) {
      return -1;
    } else {
      ByteBuffer bb = ByteBuffer.wrap(key);
      bb.getInt(); // metric id
      return bb.getLong();
    }
  }

  public static int[][] parseProperties(final byte[] key) {
    if (key == null || key.length < 12) {
      return null;
    } else {
      int length = key.length - 12;
      if (length == 0) {
        return new int[0][];
      }

      if (length % 8 != 0) {
        throw new IllegalArgumentException();
      }
      int[][] result = new int[length / 8][];
      ByteBuffer bb = ByteBuffer.wrap(key);
      bb.getInt(); // metric id
      bb.getLong(); // timestamp
      int i = 0;
      while (bb.hasRemaining()) {
        int nameid = bb.getInt();
        int valueid = bb.getInt();
        result[i] = new int[] { nameid, valueid };
        i++;
      }

      return result;
    }
  }

  private String getMetricName(int metricID) throws Exception {
    return metrics.getValue(metricID);
  }

  private Map<String, String> getTags(int[][] tags)
    throws Exception {
    if (tags == null) {
      return null;
    } else {
      Map<String, String> result = new HashMap<String, String>();
      for (int[] prop : tags) {
        String name = this.tags.getValue(prop[0]);
        String value = tagvalues.getValue(prop[1]);
        result.put(name, value);
      }
      return result;
    }
  }

  public DataPoint convert(Entry<byte[], byte[]> entry) throws Exception {
    byte[] currentKey = entry.getKey();
    byte[] dataValue = entry.getValue();
    int parsedMetricID = parseMetricID(currentKey);
    long parsedTimestamp = parseTimeStamp(currentKey);
    int[][] parsedTags = parseProperties(currentKey);

    return convert(parsedMetricID, parsedTimestamp, parsedTags,
                   dataValue);
  }

  public DataPoint convert(int parsedMetricID, long parsedTimestamp,
                            int[][] parsedTags, byte[] value) throws Exception {
    final double d;
    if (value == null || value.length != 8) {
      d = Double.NaN;
    } else {
      d = Bytes.getDouble(value);
    }

    String metricName = getMetricName(parsedMetricID);
    Map<String, String> tags = getTags(parsedTags);
    DataPoint dataPoint =
      new DataPoint.Builder(metricName).addTimestamp(parsedTimestamp)
          .addValue(d).addTags(tags).create();
    return dataPoint;
  }
}
