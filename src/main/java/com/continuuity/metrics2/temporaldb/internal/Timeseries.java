package com.continuuity.metrics2.temporaldb.internal;

import com.continuuity.metrics2.common.Zip;
import com.continuuity.metrics2.common.ZipIterator;
import com.continuuity.metrics2.temporaldb.DataPoint;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Collection of operations on Timeseries.
 */
public final class Timeseries {

  /** Divides a/b with check for Inf. */
  private static double divide(double a, double b) {
    if(b == 0) {
      b = 0.0000000001;
    }
    return a/b;
  }

  /**
   * Functor used to compute percentage.
   */
  public static class Percentage implements Function<Double, Double> {
    @Override
    public Double apply(@Nullable Double value) {
      return value * 100;
    }
  }

  /**
   * TimeseriesParallelZipIterator iterates through timeseries
   * filling in empty spaces with previous values in timeseries.
   */
  public abstract class TimeseriesParallelZipIterator
    implements ZipIterator<DataPoint, DataPoint>  {
    @Override
    public Advance advance(DataPoint a, DataPoint b) {
      if(a.getTimestamp() == b.getTimestamp()) {
        return Advance.BOTH;
      }
      if(a.getTimestamp() > b.getTimestamp()) {
        return Advance.ITER_B;
      }
      return Advance.ITER_A;
    }
  }

  /**
   * Divides the values of a timeseries by the values of another timeseries.
   * Computes [a]/[b].
   *
   * @param a list of datapoints for series A
   * @param b list of datapoints for series B
   * @return Performs a a division on each element.
   */
  public ImmutableList<DataPoint> div(final ImmutableList<DataPoint> a,
                                      final ImmutableList<DataPoint> b) {
    return div(a, b, new Function<Double, Double>() {
      @Override
      public Double apply(@Nullable Double input) {
        return input;
      }
    });
  }

  /**
   * Divides the values of a timeseries by the values of another timeseries.
   * Computes [a]/[b].
   *
   * @param a list of datapoints for series A
   * @param b list of datapoints for series B
   * @param decorator A functor used for decorating the value. E.g. compute %.
   * @return Performs a a division on each element.
   */
  public ImmutableList<DataPoint> div(final ImmutableList<DataPoint> a,
                                      final ImmutableList<DataPoint> b,
                                      final Function<Double, Double> decorator) {
    final List<DataPoint> result = new ArrayList<DataPoint>();

    // If any of the timeseries is less than 1, then we return result.
    // No point in proceeding with this.
    if(a.size() < 1 || b.size() < 1) {
      return ImmutableList.copyOf(result);
    }

    // Iterate through list in parallel based on timestamp and make
    // compute ration of a to b.
    Zip.zip(a, b, new TimeseriesParallelZipIterator() {
      @Override
      public boolean each(DataPoint a, DataPoint b, Advance advance) {
        double value = divide(a.getValue(), b.getValue());
        if(decorator != null) {
          value = decorator.apply(value);
        }
        DataPoint.Builder dp = new DataPoint.Builder(a.getMetric());
        dp.addValue(value);
        if (advance == Advance.ITER_A) {
          dp.addTimestamp(a.getTimestamp());
          dp.addTags(a.getTags());
        } else {
          dp.addTimestamp(b.getTimestamp());
          dp.addTags(b.getTags());
        }
        result.add(dp.create());
        return true;
      }
    });
    return ImmutableList.copyOf(result);
  }

  /**
   * Add  values of a timeseries by the values of another timeseries.
   * Computes [a] + [b].
   *
   * @param a list of datapoints for series A
   * @param b list of datapoints for series B
   * @return Performs a a addition on each element of list.
   */
  public ImmutableList<DataPoint> add(ImmutableList<DataPoint> a,
                                             ImmutableList<DataPoint> b) {
    final List<DataPoint> result = new ArrayList<DataPoint>();

    // If any of the timeseries is less than 1, then we return result.
    // No point in proceeding with this.
    if(a.size() < 1 || b.size() < 1) {
      return ImmutableList.copyOf(result);
    }

    // Iterate through list in parallel based on timestamp and make
    // compute addition of a to b.
    Zip.zip(a, b, new TimeseriesParallelZipIterator() {
      @Override
      public boolean each(DataPoint a, DataPoint b, Advance advance) {
        double value = a.getValue() + b.getValue();
        DataPoint.Builder dp = new DataPoint.Builder(a.getMetric());
        dp.addValue(value);
        if (advance == Advance.ITER_A) {
          dp.addTimestamp(a.getTimestamp());
          dp.addTags(a.getTags());
        } else {
          dp.addTimestamp(b.getTimestamp());
          dp.addTags(b.getTags());
        }
        result.add(dp.create());
        return true;
      }
    });
    return ImmutableList.copyOf(result);
  }


}
