package com.continuuity.metrics2.temporaldb.internal;


import com.continuuity.common.conf.CConfiguration;
import com.continuuity.metrics2.temporaldb.DataPoint;
import com.continuuity.metrics2.temporaldb.Query;
import com.continuuity.metrics2.temporaldb.Timeseries;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class LevelDBDataStoreTest {
  private static LevelDBTemporalDataStore temporalDB;
  private static File dir;
  private static long timestamp = (System.currentTimeMillis()/1000) - 24*60*60;
  private static int DATAPOINTS = 10000;

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

  @BeforeClass
  public static void beforeClass() throws Exception {
    dir = LevelDBDataStoreTest.createTempDirectory();
    temporalDB = new LevelDBTemporalDataStore(dir);
    temporalDB.open(CConfiguration.create());
    createFakeFlowDataPoints();
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
    temporalDB.put(key1.getBytes(), value1.getBytes());
    byte[] valueFromStore = temporalDB.get(key1.getBytes());
    Assert.assertNotNull(valueFromStore);
    Assert.assertEquals(value1, new String(valueFromStore));
  }

  @Test
  public void testUniqueID() throws Exception {
    String prefix = UUID.randomUUID().toString();
    UniqueId uniqueID = new UniqueId(temporalDB, prefix);
    long l = uniqueID.getOrCreateId("keyname1");
    Assert.assertEquals(1, l);
    l = uniqueID.getOrCreateId("keyname1");
    Assert.assertEquals("keyname should not force an increment", 1, l);
    l = uniqueID.getOrCreateId("keyname2");
    Assert.assertEquals("max id should have been incremented", 2, l);
    l = uniqueID.getOrCreateId("keyname2");
    Assert.assertEquals("keyname2 should not force an increment", 2, l);

    UniqueId uniqueIDWithoutCache = new UniqueId(temporalDB, prefix);
    // Assert.assertEquals("keyname",new
    // String(temporalDB.get("metrics_1".getBytes())));
    Assert.assertEquals("keyname1", uniqueIDWithoutCache.getValue(1));
    Assert.assertEquals("keyname2", uniqueIDWithoutCache.getValue(2));
    Assert.assertNull(uniqueIDWithoutCache.getValue(3));
  }

  @Test
  public void testUniqueIDMaxID() throws Exception {
    String prefix = UUID.randomUUID().toString();
    UniqueId uniqueIDWithoutCache = new UniqueId(temporalDB, prefix);

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
    UniqueId uniqueID = new UniqueId(temporalDB, prefix);
    int max = 0;
    for (int i = 0; i < 100; i++) {
      max = (i + 1);
      uniqueID.getOrCreateId(prefix + max);

    }
    UniqueId uniqueIDWithoutCache = new UniqueId(temporalDB, prefix);
    uniqueIDWithoutCache.init();
    Assert.assertEquals(prefix + "1", uniqueIDWithoutCache.getValue(1));
    Assert.assertEquals(prefix + "2", uniqueIDWithoutCache.getValue(2));
    Assert.assertNull(uniqueIDWithoutCache.getValue(max + 1));
  }

  private static void createFakeFlowDataPoints() throws Exception {
    // attr1* is flow with flowlet that three flowlets.
    // attr1* is the data for a running flow.
    Map<String, String> attr10 = Maps.newHashMap();
    attr10.put("acct", "demo");
    attr10.put("app", "abc");
    attr10.put("flow", "flow");
    attr10.put("runid", "AJKHDKJHDKJAHJS");
    attr10.put("flowlet", "flowlet1");
    attr10.put("instance", "1");

    Map<String, String> attr11 = Maps.newHashMap();
    attr11.put("acct", "demo");
    attr11.put("app", "abc");
    attr11.put("flow", "flow");
    attr11.put("runid", "AJKHDKJHDKJAHJS");
    attr11.put("flowlet", "flowlet2");
    attr11.put("instance", "1");

    // Same as above, but flowlet instance 2
    Map<String, String> attr12 = Maps.newHashMap();
    attr12.put("acct", "demo");
    attr12.put("app", "abc");
    attr12.put("flow", "flow");
    attr12.put("runid", "AJKHDKJHDKJAHJS");
    attr12.put("flowlet", "flowlet1");
    attr12.put("instance", "2");

    Map<String, String> attr13 = Maps.newHashMap();
    attr13.put("acct", "demo");
    attr13.put("app", "abc");
    attr13.put("flow", "flow");
    attr13.put("runid", "AJKHDKJHDKJAHJS");
    attr13.put("flowlet", "flowlet2");
    attr13.put("instance", "2");

    // Add datapoints for a flow xyz.
    Map<String, String> attr20 = Maps.newHashMap();
    attr20.put("acct", "demo");
    attr20.put("app", "xyz");
    attr20.put("flow", "flow");
    attr20.put("runid", "FGH");
    attr20.put("flowlet", "flowlet1");
    attr20.put("instance", "1");

    Map<String, String> attr21 = Maps.newHashMap();
    attr21.put("acct", "demo");
    attr21.put("app", "xyz");
    attr21.put("flow", "flow");
    attr21.put("runid", "FGH");
    attr21.put("flowlet", "flowlet2");
    attr21.put("instance", "1");

    // Add datapoints for a account wtf.
    Map<String, String> attr30 = Maps.newHashMap();
    attr30.put("acct", "wtf");
    attr30.put("app", "xyz");
    attr30.put("flow", "flow");
    attr30.put("runid", "FGH");
    attr30.put("flowlet", "flowlet1");
    attr30.put("instance", "1");

    Map<String, String> attr31 = Maps.newHashMap();
    attr31.put("acct", "wtf");
    attr31.put("app", "xyz");
    attr31.put("flow", "flow");
    attr31.put("runid", "FGH");
    attr31.put("flowlet", "flowlet2");
    attr31.put("instance", "1");

    // Add datapoints for a flow fal.
    Map<String, String> attr40 = Maps.newHashMap();
    attr40.put("acct", "wtf");
    attr40.put("app", "fal");
    attr40.put("flow", "flow");
    attr40.put("runid", "FGH");
    attr40.put("flowlet", "flowlet1");
    attr40.put("instance", "1");

    Map<String, String> attr41 = Maps.newHashMap();
    attr41.put("acct", "wtf");
    attr41.put("app", "fal");
    attr41.put("flow", "flow");
    attr41.put("runid", "FGH");
    attr41.put("flowlet", "flowlet2");
    attr41.put("instance", "1");

    long movingTimestamp = timestamp;
    for(int i = 0; i < DATAPOINTS; ++i) {
      {
        DataPoint point1 = new DataPoint.Builder("tuple.read")
                .addTimestamp(movingTimestamp).addValue(i).addTags(attr10).create();
        DataPoint point2 = new DataPoint.Builder("tuple.read")
                .addTimestamp(movingTimestamp).addValue(i).addTags(attr11).create();
        temporalDB.put(point1);
        temporalDB.put(point2);
      }
      {
        DataPoint point1 = new DataPoint.Builder("tuple.proc")
          .addTimestamp(movingTimestamp).addValue(i).addTags(attr10).create();
        DataPoint point2 = new DataPoint.Builder("tuple.proc")
          .addTimestamp(movingTimestamp).addValue(i).addTags(attr11).create();
        temporalDB.put(point1);
        temporalDB.put(point2);
      }
      movingTimestamp++;
    }

    movingTimestamp = timestamp;
    for(int i = 0; i < DATAPOINTS; ++i) {
      {
        DataPoint point1 = new DataPoint.Builder("tuple.read")
          .addTimestamp(movingTimestamp).addValue(i).addTags(attr12).create();
        DataPoint point2 = new DataPoint.Builder("tuple.read")
          .addTimestamp(movingTimestamp).addValue(i).addTags(attr13).create();
        temporalDB.put(point1);
        temporalDB.put(point2);
      }
      {
        DataPoint point1 = new DataPoint.Builder("tuple.proc")
          .addTimestamp(movingTimestamp).addValue(i).addTags(attr12).create();
        DataPoint point2 = new DataPoint.Builder("tuple.proc")
          .addTimestamp(movingTimestamp).addValue(i).addTags(attr13).create();
        temporalDB.put(point1);
        temporalDB.put(point2);
      }
      movingTimestamp++;
    }

    movingTimestamp = timestamp;
    for(int i = 0; i < DATAPOINTS; ++i) {
      {
        DataPoint point1 = new DataPoint.Builder("tuple.read")
          .addTimestamp(movingTimestamp).addValue(i).addTags(attr20).create();
        DataPoint point2 = new DataPoint.Builder("tuple.read")
          .addTimestamp(movingTimestamp).addValue(i).addTags(attr21).create();
        temporalDB.put(point1);
        temporalDB.put(point2);
      }
      {
        DataPoint point1 = new DataPoint.Builder("tuple.proc")
          .addTimestamp(movingTimestamp).addValue(i).addTags(attr20).create();
        DataPoint point2 = new DataPoint.Builder("tuple.proc")
          .addTimestamp(movingTimestamp).addValue(i).addTags(attr21).create();
        temporalDB.put(point1);
        temporalDB.put(point2);
      }
      movingTimestamp++;
    }

    movingTimestamp = timestamp;
    for(int i = 0; i < DATAPOINTS; ++i) {
      {
        DataPoint point1 = new DataPoint.Builder("tuple.read")
          .addTimestamp(movingTimestamp).addValue(i).addTags(attr30).create();
        DataPoint point2 = new DataPoint.Builder("tuple.read")
          .addTimestamp(movingTimestamp).addValue(i).addTags(attr31).create();
        temporalDB.put(point1);
        temporalDB.put(point2);
      }
      {
        DataPoint point1 = new DataPoint.Builder("tuple.proc")
          .addTimestamp(movingTimestamp).addValue(i).addTags(attr30).create();
        DataPoint point2 = new DataPoint.Builder("tuple.proc")
          .addTimestamp(movingTimestamp).addValue(i).addTags(attr31).create();
        temporalDB.put(point1);
        temporalDB.put(point2);
      }
      movingTimestamp++;
    }

    movingTimestamp = timestamp;
    for(int i = 0; i < DATAPOINTS; ++i) {
      {
        DataPoint point1 = new DataPoint.Builder("tuple.read")
          .addTimestamp(movingTimestamp).addValue(i).addTags(attr40).create();
        DataPoint point2 = new DataPoint.Builder("tuple.read")
          .addTimestamp(movingTimestamp).addValue(i).addTags(attr41).create();
        temporalDB.put(point1);
        temporalDB.put(point2);
      }
      {
        DataPoint point1 = new DataPoint.Builder("tuple.proc")
          .addTimestamp(movingTimestamp).addValue(i).addTags(attr40).create();
        DataPoint point2 = new DataPoint.Builder("tuple.proc")
          .addTimestamp(movingTimestamp).addValue(i).addTags(attr41).create();
        temporalDB.put(point1);
        temporalDB.put(point2);
      }
      movingTimestamp++;
    }

  }

  /**
   * Reads a single flowlet metric for a given instance.
   *
   * @throws Exception
   */
  @Test
  public void testReadSingleFlowletMetricForAFlow() throws Exception {
    Query query = new Query.select("tuple.read")
      .from(timestamp)
      .to(timestamp + DATAPOINTS)
      .and()
      .has("acct", "demo")
      .has("app", "abc")
      .has("flow", "flow")
      .has("runid", "AJKHDKJHDKJAHJS")
      .has("flowlet", "flowlet1")
      .has("instance", "1")
      .create();
    ImmutableList<DataPoint> points = temporalDB.execute(query);
    Assert.assertNotNull(points);
    double count = 0;
    for(DataPoint point : points) {
      Assert.assertTrue(count == point.getValue());
      count++;
    }
  }

  /**
   * Reads timeseries across multiple flowlet instances. attr10, attr12.
   * Tests Aggregate across multiple instance under a account, app, flow,
   * runid, flowlet.
   *
   * @throws Exception
   */
  @Test
  public void testReadMultipleFlowletMetricForAFlow() throws Exception {
    Query query = new Query.select("tuple.read")
      .from(timestamp)
      .to(timestamp + DATAPOINTS)
      .and()
      .has("acct", "demo")
      .has("app", "abc")
      .has("flow", "flow")
      .has("runid", "AJKHDKJHDKJAHJS")
      .has("flowlet", "flowlet1")
      .create();
    ImmutableList<DataPoint> points = temporalDB.execute(query);
    Assert.assertNotNull(points);
    double count = 0;
    for(DataPoint point : points) {
      Assert.assertTrue(count*2 == point.getValue());
      count++;
    }
  }

  /**
   * Test a query that is aggregating metrics across multiple flowlets in a
   * flow. It also aggregates over multiple instances within a flowlet.
   *
   * @throws Exception
   */
  @Test
  public void testQueryAcrossAllFlowletsInAFlow() throws Exception {
    Query query =
    new Query.select("tuple.read")
      .from(timestamp)
      .to(timestamp + DATAPOINTS)
      .and()
      .has("acct", "demo")
      .has("app", "abc")
      .has("flow", "flow")
      .has("runid", "AJKHDKJHDKJAHJS")
      .create();
    ImmutableList<DataPoint> points = temporalDB.execute(query);

    Assert.assertNotNull(points);
    double count = 0;
    for(DataPoint point : points) {
      Assert.assertTrue(count*4 == point.getValue());
      count++;
    }
  }

  @Test
  public void testComputingBusynessMetricForAFlowAtFlowletLevel()
      throws Exception {
    Query queryA = new Query.select("tuple.read")
      .from(timestamp)
      .to(timestamp + DATAPOINTS)
      .and()
      .has("acct", "demo")
      .has("app", "abc")
      .has("flow", "flow")
      .has("runid", "AJKHDKJHDKJAHJS")
      .has("flowlet", "flowlet1")
      .create();

    ImmutableList<DataPoint> a = temporalDB.execute(queryA);

    Query queryB = new Query.select("tuple.proc")
      .from(timestamp)
      .to(timestamp + DATAPOINTS)
      .and()
      .has("acct", "demo")
      .has("app", "abc")
      .has("flow", "flow")
      .has("runid", "AJKHDKJHDKJAHJS")
      .has("flowlet", "flowlet1")
      .create();

    ImmutableList<DataPoint> b = temporalDB.execute(queryB);

    ImmutableList<DataPoint> busyness =
      new Timeseries().div(a, b);
    int idx = 0;
    for(DataPoint p : busyness) {
      if(idx == 0) {
        Assert.assertTrue(p.getValue() == 0);
        idx = 1;
      } else {
        Assert.assertTrue(p.getValue() == 1);
      }
    }
  }

  /**
   * Tests how busyness metrics can be computed. Computing busyness
   * requires us to get two metrics. 1. tuple.read and 2. tuple.proc.
   * So, busyness = ( tuple.proc / tuple.read ) * 1000;
   *
   * @throws Exception
   */
  @Test
  public void testComputingBusynessMetricForAFlowAtFlowLevel()
    throws Exception {
    Query queryA = new Query.select("tuple.read")
      .from(timestamp)
      .to(timestamp + DATAPOINTS)
      .and()
      .has("acct", "demo")
      .has("app", "abc")
      .has("flow", "flow")
      .has("runid", "AJKHDKJHDKJAHJS")
      .create();

    ImmutableList<DataPoint> a = temporalDB.execute(queryA);

    Query queryB = new Query.select("tuple.proc")
      .from(timestamp)
      .to(timestamp + DATAPOINTS)
      .and()
      .has("acct", "demo")
      .has("app", "abc")
      .has("flow", "flow")
      .has("runid", "AJKHDKJHDKJAHJS")
      .create();

    ImmutableList<DataPoint> b = temporalDB.execute(queryB);
    ImmutableList<DataPoint> busyness =
      new Timeseries().div(a, b, new Timeseries.Percentage());
    int idx = 0;
    for(DataPoint p : busyness) {
      if(idx == 0) {
        Assert.assertTrue(p.getValue() == 0);
        idx = 1;
      } else {
        Assert.assertTrue(p.getValue() == 100);
      }
    }
  }
}
