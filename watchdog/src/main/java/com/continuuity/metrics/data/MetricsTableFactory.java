/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

/**
 * Factory to encapsulate creation of {@link TimeSeriesTable}.
 */
public interface MetricsTableFactory {

  /**
   * Creates a new instance of {@link TimeSeriesTable} with the given resolution.
   * @param resolution The resolution that the table represents.
   * @return A new instance of {@link TimeSeriesTable}.
   */
  TimeSeriesTable createTimeSeries(int resolution);
}
