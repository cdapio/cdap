package com.continuuity.metrics2.common;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;

public class LevelDBDataStoreTest {
  private static LevelDBDataStore store;
  private static File dir;
  private static List<DataPoint> dataPoints;

  public static File createTempDirectory() throws IOException {
    final File temp;
    temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
    if (!(temp.delete())) {
      throw new IOException("Could not delete temp file: "
                              + temp.getAbsolutePath());
    }
    if (!(temp.mkdir())) {
      throw new IOException("Could not create temp directory: "
                              + temp.getAbsolutePath());
    }
    return (temp);
  }

  public static List<DataPoint> fakeDataPoints(int count){
    List<DataPoint> dataPoints = new ArrayList<DataPoint>(count);
    long startTs = System.currentTimeMillis() - (1000 * 60 * 60 * 24);
    long ts = startTs;
    double startValue = 1d;
    double value = startValue;
    Map<String, String> properties = new HashMap<String, String>();
    properties.put("prop1", "propvalue1");
    properties.put("prop2", "propvalue2");
    for (int i = 0; i < count; i++) {
      ts = ts + 100;
      value = value + 1;
      DataPoint dp = new DataPointImpl("metric", ts, value, properties);
      dataPoints.add(dp);
    }
    return dataPoints;
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    File dir = LevelDBDataStoreTest.createTempDirectory();
    store = new LevelDBDataStore(dir);
    store.setDbFactory(Iq80DBFactory.factory);
    store.open();

    int count = 100000;
    dataPoints = LevelDBDataStoreTest.fakeDataPoints(count);

  }

  @AfterClass
  public static void afterClass() throws IOException {
    if (dir != null) {
      dir.delete();
    }
  }

  @Test
  public void testPutAndGetSingle() throws Exception {
    String key1 = new String("key1");
    String value1 = new String("value1");
    store.put(key1.getBytes(), value1.getBytes());
    byte[] valueFromStore = store.get(key1.getBytes());
    Assert.assertNotNull(valueFromStore);
    Assert.assertEquals(value1, new String(valueFromStore));
  }

  @Test
  public void testPutDataPoints() throws Exception {
    store.putMultiple(dataPoints);
  }

  @Test
  public void testGetDataPoints() throws Exception {
    store.putMultiple(dataPoints);
    Query query = new QueryImpl();
    query.setMetricName(dataPoints
                          .get(0).getMetric());
    query.setStartTime(dataPoints.get(0).getTimestamp());
    query.setEndTime(System.currentTimeMillis()+100000);

    List<DataPoint> retrievedPoints = store.getDataPoints(query);
    Assert.assertNotNull(retrievedPoints);
    Assert.assertEquals(dataPoints.size(), retrievedPoints.size());

    for (DataPoint dp : retrievedPoints) {
      Assert.assertEquals("metric", dp.getMetric());
      Assert.assertNotNull("tags", dp.getTags());
      Assert.assertEquals("propvalue1", dp.getTags().get("prop1"));
      Assert.assertEquals("propvalue2", dp.getTags().get("prop2"));
    }
  }

  @Test
  public void testUniqueID() throws Exception {
    String prefix = UUID.randomUUID().toString();
    UniqueId uniqueID = new UniqueId(store, prefix);
    long l = uniqueID.getOrCreateId("keyname1");
    Assert.assertEquals(1, l);
    l = uniqueID.getOrCreateId("keyname1");
    Assert.assertEquals("keyname should not force an increment", 1, l);
    l = uniqueID.getOrCreateId("keyname2");
    Assert.assertEquals("max id should have been incremented", 2, l);
    l = uniqueID.getOrCreateId("keyname2");
    Assert.assertEquals("keyname2 should not force an increment", 2, l);

    UniqueId uniqueIDWithoutCache = new UniqueId(store, prefix);
    // Assert.assertEquals("keyname",new
    // String(store.get("metrics_1".getBytes())));
    Assert.assertEquals("keyname1", uniqueIDWithoutCache.getValue(1));
    Assert.assertEquals("keyname2", uniqueIDWithoutCache.getValue(2));
    Assert.assertNull(uniqueIDWithoutCache.getValue(3));

  }

  @Test
  public void testUniqueIDMaxID() throws Exception {
    String prefix = UUID.randomUUID().toString();
    UniqueId uniqueIDWithoutCache = new UniqueId(store, prefix);

    uniqueIDWithoutCache.setMaxID(100);
    Assert.assertEquals(100, uniqueIDWithoutCache.getMaxID());
    Assert.assertEquals(100, uniqueIDWithoutCache.getMaxID());

    uniqueIDWithoutCache.setMaxID(101);
    Assert.assertEquals(101, uniqueIDWithoutCache.getMaxID());

    uniqueIDWithoutCache.setMaxID(102);
    Assert.assertEquals(102, uniqueIDWithoutCache.getMaxID());
    Assert.assertEquals(102, uniqueIDWithoutCache.getMaxID());

    uniqueIDWithoutCache.setMaxID(1);
    Assert.assertEquals(1, uniqueIDWithoutCache.getMaxID());
  }

  @Test
  public void testUniqueIDFillCache() throws Exception {
    String prefix = UUID.randomUUID().toString();
    UniqueId uniqueID = new UniqueId(store, prefix);
    int max = 0;
    for (int i = 0; i < 100; i++) {
      max = (i + 1);
      uniqueID.getOrCreateId(prefix + max);

    }

    UniqueId uniqueIDWithoutCache = new UniqueId(store, prefix);
    uniqueIDWithoutCache.init();
    Assert.assertEquals(prefix + "1", uniqueIDWithoutCache.getValue(1));
    Assert.assertEquals(prefix + "2", uniqueIDWithoutCache.getValue(2));
    Assert.assertNull(uniqueIDWithoutCache.getValue(max + 1));
  }

  @Test
  public void testFlowMetrics() throws Exception {
    Map<String, String> attributes = Maps.newHashMap();
    attributes.put("acct", "demo");
    attributes.put("app", "abc");
    attributes.put("flow", "flow");
    attributes.put("runid", "AJKHDKJHDKJAHJS");
    attributes.put("flowlet", "flowlet");
    attributes.put("instance", "1");

    long timestamp = System.currentTimeMillis()/1000;
    for(int i = 0; i < 5; ++i) {
      DataPoint point = new DataPointImpl("processed.count", timestamp, i,
                                          attributes);
      timestamp++;
      store.put(point);
    }

    Query query = new QueryImpl();
    query.setMetricName("processed.count");
    query.setStartTime(timestamp-1000);
    query.setEndTime(timestamp+1000);
    query.setTagFilter(attributes);
    List<DataPoint> points = store.getDataPoints(query);
    Assert.assertNotNull(points);
    Assert.assertTrue(points.size() > 1);

    attributes.put("flow", "flow2");
    for(int i = 0; i < 10; ++i) {
      DataPoint point = new DataPointImpl("processed.count", timestamp, i,
                                          attributes);
      timestamp++;
      store.put(point);
    }

    long start = System.nanoTime();
    for(int i = 0; i < 1000; ++i) {
      if(i % 1000 == 0) {
        System.out.println("Processed " + i);
      }
      attributes.put("flow", "*");
      query = new QueryImpl();
      query.setMetricName("processed.count");
      query.setStartTime(timestamp-1000);
      query.setEndTime(timestamp+1000);
      query.setTagFilter(attributes);
      final Map<String, List<DataPoint>> pointMap = Maps.newHashMap();
      query.setCallback(new Function<DataPoint, Void>() {
        @Override
        public Void apply(@Nullable DataPoint input) {
          String metric = input.getMetric();
          if(! pointMap.containsKey(metric)) {
            pointMap.put(metric, new ArrayList<DataPoint>());
          }
          pointMap.get(metric).add(input);
          return null;
        }
      });
      points = store.getDataPoints(query);
      Assert.assertNotNull(points);
      Assert.assertTrue(points.size() > 1);
    }
    long end = System.nanoTime();
    System.out.println( "Time in ns = " + (end - start));

    attributes.put("flow", "*");
    query = new QueryImpl();
    query.setMetricName("processed.count");
    query.setStartTime(timestamp-1000);
    query.setEndTime(timestamp+1000);
    query.setTagFilter(attributes);
    points = store.getDataPoints(query);
    Assert.assertNotNull(points);
    Assert.assertTrue(points.size() > 1);
  }
}
