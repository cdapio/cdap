package com.continuuity.metrics2.temporaldb.internal;

import com.continuuity.metrics2.common.Zip;
import com.continuuity.metrics2.common.ZipIterator;
import com.continuuity.metrics2.temporaldb.DataPoint;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 *
 *
 */
public class DataPointUtilities {

  /**
   * Adds values of two time series.
   *
   * @param point1
   * @param point2
   * @return
   */
  public static ImmutableList<DataPoint>
      add(ImmutableList<DataPoint> point1,
          ImmutableList<DataPoint> point2) {

    final List<DataPoint> result = Lists.newArrayList();
    boolean status =
      Zip.zip(point1, point2, new ZipIterator<DataPoint, DataPoint>() {
      @Override
      public boolean each(DataPoint dataPoint1, DataPoint dataPoint2) {
        long timestamp1 = dataPoint1.getTimestamp();
        long timestamp2 = dataPoint2.getTimestamp();
        if(timestamp1 == timestamp2) {
          DataPoint dp = new DataPoint.Builder(dataPoint1.getMetric())
            .addTimestamp(timestamp1).addValue(dataPoint1.getValue() +
                dataPoint2.getValue())
            .addTags(dataPoint1.getTags())
            .create();
          result.add(dp);
        } else if(timestamp1 > timestamp2) {
          result.add(dataPoint2);
        } else {
          result.add(dataPoint1);
        }
        return true;
      }
    });

    return null;
  }

  public static List<DataPoint> multiply(List<DataPoint> point1,
                                         List<DataPoint> points) {
    return null;
  }

}
