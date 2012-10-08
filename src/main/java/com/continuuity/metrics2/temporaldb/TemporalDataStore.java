package com.continuuity.metrics2.temporaldb;

import com.google.common.collect.ImmutableList;

/**
 * Interface specifying the datastore.
 */
public interface TemporalDataStore {
  /**
   * Opens a datastore.
   * @throws Exception
   */
  public void open() throws Exception;

  /**
   * Puts a {@link DataPoint} into the store.
   *
   * @param point to be added to store.
   * @throws Exception
   */
  public void put(DataPoint point) throws Exception;

  /**
   * Given a {@link Query} returns a list of {@link DataPoint}.
   *
   * @param query to be used retrieve {@link DataPoint}
   * @return list of datapoints.
   * @throws Exception
   */
  public ImmutableList<DataPoint> getDataPoints(Query query) throws Exception;

  /**
   * Closes the datastore.
   * @throws Exception
   */
  public void close() throws Exception;
}
