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
    long location = 1L;
    Map<String, Long> locations = Maps.newHashMap();
    locations.put("A", location);

    byte[] data = "SERIALIZED-TUPLE".getBytes();

    byte[]  b = TupleMetaDataAnnotator.EnqueuePayload.write(
      locations, data
    );

    TupleMetaDataAnnotator.EnqueuePayload enqueuePayload =
      TupleMetaDataAnnotator.EnqueuePayload.read(b);

    Assert.assertTrue(Bytes.compareTo(enqueuePayload.getSerializedTuple(), data) == 0);
    Assert.assertTrue(enqueuePayload.getOperationIds().get("A") == location);
  }

  @Test
  public void DequeueWithMetaData() throws Exception {
    long value = 1;
    Map<String, Long> values = Maps.newHashMap();
    values.put("A", value);

    byte[] data = "SERIALIZED-TUPLE".getBytes();

    byte[]  b = TupleMetaDataAnnotator.DequeuePayload.write(
      values, data
    );

    TupleMetaDataAnnotator.DequeuePayload dequeuePayload =
      TupleMetaDataAnnotator.DequeuePayload.read(b);

    Assert.assertTrue(Bytes.compareTo(dequeuePayload.getSerializedTuple(), data) == 0);
    Assert.assertTrue(dequeuePayload.getValues().get("A") == value);
  }

}
