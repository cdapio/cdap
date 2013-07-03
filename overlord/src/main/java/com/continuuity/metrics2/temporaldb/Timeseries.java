package com.continuuity.metrics2.temporaldb;

import com.continuuity.metrics2.common.Zip;
import com.continuuity.metrics2.common.ZipIterator;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * Collection of operations on Timeseries.
 */
public final class Timeseries {

  /** Divides a/b with check for Inf. */
  private static double divide(double a, double b) {
    if (b == 0) {
      b = 0.0000000001;
    }
    return a / b;
  }

  /**
   * Functor used to compute percentage.
   */
  public static class Percentage implements Function<Double, Double> {
    @Override
    public Double apply(Double value) {
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
      if (a.getTimestamp() == b.getTimestamp()) {
        return Advance.BOTH;
      }
      if (a.getTimestamp() > b.getTimestamp()) {
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
      public Double apply(Double input) {
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
    if (a == null || b == null || a.size() < 1 || b.size() < 1) {
      return ImmutableList.copyOf(result);
    }

    // Iterate through list in parallel based on timestamp and make
    // compute ration of a to b.
    Zip.zip(a, b, new TimeseriesParallelZipIterator() {
      @Override
      public boolean each(DataPoint a, DataPoint b, Advance advance) {
        double value = divide(a.getValue(), b.getValue());
        if (decorator != null) {
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
    if (a.size() < 1 || b.size() < 1) {
      return ImmutableList.copyOf(result);
    }

    // Iterate through list in parallel based on timestamp and
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

  /**
   * Converts a list of data point to rate.
   *
   * @param a immutable list of datapoint
   * @return immutable list of datapoint converted to rate.
   */
  public ImmutableList<DataPoint> rate(List<DataPoint> a) {
    List<DataPoint> result = new ArrayList<DataPoint>();
    if (a == null) {
      return ImmutableList.copyOf(result);
    }
    DataPoint prev = null;

    for (DataPoint dataPoint : a) {
      if (prev == null) {
        prev = dataPoint;
      }
      DataPoint.Builder dpb = new DataPoint.Builder(dataPoint.getMetric());
      dpb.addTags(dataPoint.getTags());
      dpb.addTimestamp(dataPoint.getTimestamp());
      dpb.addValue(Math.abs(prev.getValue() - dataPoint.getValue()));
      result.add(dpb.create());
      prev = dataPoint;
    }
    if (result.size() > 1) {
      DataPoint p = result.get(1);
      result.remove(0);
      result.add(0, p);
    }
    return ImmutableList.copyOf(result);
  }

  /**
   * Fills the list of datapoint with zeros.
   * NOTE: This is a crappy function. Need more smarts.
   *
   * @param a list of datapoints to be filled with zeros.
   * @param start time at which we start filling zeros.
   * @param points specifies number of points.
   * @param interval increments at which datapoints are added.
   * @return immutable list of datapoints filled with zeros.
   */
  public ImmutableList<DataPoint> fill(List<DataPoint> a,
                                       String metric,
                                       long start, long end,
                                       long points, long interval) {
    List<DataPoint> result = new ArrayList<DataPoint>();
    if (a == null || a.size() < 1) {
      DataPoint.Builder dpb = new DataPoint.Builder(metric);
      for (long i = start; i < start + points; i = i + interval) {
        dpb.addTimestamp(i);
        dpb.addValue((double) 0);
        result.add(dpb.create());
      }
      return ImmutableList.copyOf(result);
    } else {
      long startTimestamp = a.get(0).getTimestamp();
      long endTimestamp = a.get(a.size() - 1).getTimestamp();
      if (start < startTimestamp) {
        DataPoint.Builder dpb = new DataPoint.Builder(metric);
        for (long i = start; i < startTimestamp; i = i + interval) {
          dpb.addTimestamp(i);
          dpb.addValue((double) 0);
          result.add(dpb.create());
        }
      }
      result.addAll(a);
      if (endTimestamp < end) {
        DataPoint.Builder dpb = new DataPoint.Builder(metric);
        for (long i = endTimestamp + interval; i < end; i = i + interval) {
          dpb.addTimestamp(i);
          dpb.addValue((double) 0);
          result.add(dpb.create());
//          size++;
//          if(size >= points) {
//            break;
//          }
        }
      }
//
//      boolean addedAtStart = false;
//      if(start < startTimestamp) {
//        DataPoint.Builder dpb = new DataPoint.Builder(metric);
//        for(long i = start; i < startTimestamp; i = i + 1) {
//          dpb.addTimestamp(i);
//          dpb.addValue(new Double(0));
//          result.add(dpb.create());
//        }
//        addedAtStart = true;
//      }
//      result.addAll(a);
//      if(!addedAtStart) {
//        int size = result.size();
//        long timestamp = a.get(a.size()-1).getTimestamp();
//        DataPoint.Builder dpb = new DataPoint.Builder(metric);
//        for(int i = size + 1; i <= points; ++i) {
//          dpb.addTimestamp(timestamp + i - size);
//          dpb.addValue(new Double(0));
//          result.add(dpb.create());
//        }
//      }
    }
    return ImmutableList.copyOf(result);
  }

}
