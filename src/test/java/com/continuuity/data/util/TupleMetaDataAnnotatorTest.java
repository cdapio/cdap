package com.continuuity.data.util;

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Tests Tuple Meta Data Annotator.
 */
public class TupleMetaDataAnnotatorTest {

  @Test
  public void EnqueueWithMetaData() throws Exception {
    short location = 1;
    Map<String, Short> locations = Maps.newHashMap();
    locations.put("A", location);

    byte[] data = "SERIALIZED-TUPLE".getBytes();

    byte[]  b = TupleMetaDataAnnotator.Enqueue.write(
      locations, data
    );

    TupleMetaDataAnnotator.Enqueue enqueue =
      TupleMetaDataAnnotator.Enqueue.read(b);

    Assert.assertTrue(Bytes.compareTo(enqueue.getSerializedTuple(), data) == 0);
    Assert.assertTrue(enqueue.getLocations().get("A") == location);
  }

  @Test
  public void DequeueWithMetaData() throws Exception {
    long value = 1;
    Map<String, Long> values = Maps.newHashMap();
    values.put("A", value);

    byte[] data = "SERIALIZED-TUPLE".getBytes();

    byte[]  b = TupleMetaDataAnnotator.Dequeue.write(
      values, data
    );

    TupleMetaDataAnnotator.Dequeue dequeue =
      TupleMetaDataAnnotator.Dequeue.read(b);

    Assert.assertTrue(Bytes.compareTo(dequeue.getSerializedTuple(), data) == 0);
    Assert.assertTrue(dequeue.getValues().get("A") == value);
  }

}
