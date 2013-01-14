package com.continuuity.metrics2.temporaldb.internal;

import com.continuuity.metrics2.temporaldb.DataPoint;
import com.continuuity.metrics2.temporaldb.Timeseries;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests operations within timeseries.
 */
public class TimeseriesTest {

  /** Creates a {@link DataPoint} instance. */
  private DataPoint create(String name, long timestamp, double value) {
    return new DataPoint.Builder(name)
      .addTimestamp(timestamp).addValue(value).create();
  }

  /**
   * Tests when we have timeseries of the same size.
   */
  @Test
  public void testSameSizeTimeSeries() throws Exception {
    List<DataPoint> A = Lists.newArrayList();
    List<DataPoint> B = Lists.newArrayList();
    A.add(create("A", 1, 2));
    A.add(create("A", 2, 2));
    A.add(create("A", 3, 2));
    A.add(create("A", 4, 2));
    A.add(create("A", 5, 2));
    A.add(create("A", 6, 2));
    A.add(create("A", 7, 2));
    B.add(create("A", 1, 4));
    B.add(create("A", 2, 4));
    B.add(create("A", 3, 4));
    B.add(create("A", 4, 4));
    B.add(create("A", 5, 4));
    B.add(create("A", 6, 4));
    B.add(create("A", 7, 4));

    ImmutableList<DataPoint> div =
      new Timeseries().div(ImmutableList.copyOf(A), ImmutableList.copyOf(B));

    Assert.assertNotNull(div);
    Assert.assertTrue(div.size() == 7);

    for(DataPoint d : div) {
      Assert.assertTrue(d.getValue() == 0.5f);
    }
  }

  /**
   * Test when timeseries A has fewer elements than timeseries B. In this
   * case last value of timeseries A should be used for computing the value.
   * @throws Exception
   */
  @Test
  public void testSeriesASmallerThanB() throws Exception {
    List<DataPoint> A = Lists.newArrayList();
    List<DataPoint> B = Lists.newArrayList();
    A.add(create("A", 1, 2));
    A.add(create("A", 2, 2));
    A.add(create("A", 3, 2));

    B.add(create("A", 1, 4));
    B.add(create("A", 2, 4));
    B.add(create("A", 3, 4));
    B.add(create("A", 4, 4));
    B.add(create("A", 5, 4));
    B.add(create("A", 6, 4));
    B.add(create("A", 7, 4));

    ImmutableList<DataPoint> div =
      new Timeseries().div(ImmutableList.copyOf(A), ImmutableList.copyOf(B));

    Assert.assertNotNull(div);
    Assert.assertTrue(div.size() == 7);

    for(DataPoint d : div) {
      Assert.assertTrue(d.getValue() == 0.5f);
    }
  }

  /**
   * Test when timeseries B has fewer elements than timeseries A. In this
   * case last value of timeseries B should be used for computing the value.
   */
  @Test
  public void testSeriesBSmallerThanA() throws Exception {
    List<DataPoint> A = Lists.newArrayList();
    List<DataPoint> B = Lists.newArrayList();
    A.add(create("A", 1, 2));
    A.add(create("A", 2, 2));
    A.add(create("A", 3, 2));
    A.add(create("A", 4, 2));
    A.add(create("A", 5, 2));
    A.add(create("A", 6, 2));
    A.add(create("A", 7, 2));
    B.add(create("A", 1, 4));
    B.add(create("A", 2, 4));

    ImmutableList<DataPoint> div =
      new Timeseries().div(ImmutableList.copyOf(A), ImmutableList.copyOf(B));

    Assert.assertNotNull(div);
    Assert.assertTrue(div.size() == 7);

    for(DataPoint d : div) {
      Assert.assertTrue(d.getValue() == 0.5f);
    }
  }

  /**
   * Both Series have same number of points the timestamp's don't align.
   */
  @Test
  public void testSeriesWithMismatchTimestamps() throws Exception {
    List<DataPoint> A = Lists.newArrayList();
    List<DataPoint> B = Lists.newArrayList();
    A.add(create("A", 1, 2));
    A.add(create("A", 2, 2));
    A.add(create("A", 4, 2));
    A.add(create("A", 5, 2));

    B.add(create("A", 1, 4));
    B.add(create("A", 2, 4));
    B.add(create("A", 3, 4));
    B.add(create("A", 4, 4));

    ImmutableList<DataPoint> div =
      new Timeseries().div(ImmutableList.copyOf(A), ImmutableList.copyOf(B),
                           new Function<Double, Double>() {
                             @Override
                             public Double apply(Double value) {
                              return value * 100;
                             }
                           });

    Assert.assertNotNull(div);
    Assert.assertTrue(div.size() == 5);

    for(DataPoint d : div) {
      Assert.assertTrue(d.getValue() == 50f);
    }
  }

  /**
   * Test adding zeros to timeseries when the list is no filled completely.
   */
  @Test
  public void testAddingZerosToTimeseries() throws Exception {
    List<DataPoint> A = Lists.newArrayList();
    A.add(create("A", 1, 2));
    A.add(create("A", 2, 2));
    A.add(create("A", 3, 2));
    A.add(create("A", 4, 2));
    A.add(create("A", 5, 2));

    ImmutableList<DataPoint> zeroes =
      new Timeseries().fill(A, "A", 1, 10, 10, 1);
    Assert.assertNotNull(zeroes);
    Assert.assertTrue(zeroes.size() > 1);
  }

  /**
   * Test adding zeros to timeseries when the list had missing timestamp
   * at begining of timeseries
   */
  @Test
  public void testAddingZerosAtBeginingTimeseries() throws Exception {
    List<DataPoint> A = Lists.newArrayList();
    A.add(create("A", 5, 2));
    A.add(create("A", 6, 2));
    A.add(create("A", 7, 2));
    A.add(create("A", 8, 2));
    A.add(create("A", 9, 2));

    ImmutableList<DataPoint> zeroes =
      new Timeseries().fill(A, "A", 0, 10, 10, 1);
    Assert.assertNotNull(zeroes);
    Assert.assertTrue(zeroes.size() > 1);
  }

  /**
   * Test adding zeros to timeseries when list is empty
   */
  @Test
  public void testAddingZerosToEmptyTimeseries() throws Exception {
    List<DataPoint> A = Lists.newArrayList();
    ImmutableList<DataPoint> zeroes =
      new Timeseries().fill(A, "A", 1, 10, 10, 1);
    Assert.assertNotNull(zeroes);
    Assert.assertTrue(zeroes.size() == 10);
    A = null;
    zeroes =
      new Timeseries().fill(A, "A", 1, 10, 10, 1);
    Assert.assertNotNull(zeroes);
    Assert.assertTrue(zeroes.size() > 1);
  }
}
