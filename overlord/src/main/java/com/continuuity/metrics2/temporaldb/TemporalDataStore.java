package com.continuuity.metrics2.temporaldb;

import com.continuuity.common.conf.CConfiguration;
import com.google.common.collect.ImmutableList;

import java.io.Closeable;

/**
 * TemporalDataStore specifies the interface for writing timeseries
 * data into a store. It supports ability to add a {@code DataPoint}
 * and execute a {@code Query}. {@code TemporalDataStore}'s underlying
 * storage is a Key-Value store.
 */
public interface TemporalDataStore extends Closeable {
  /**
   * Opens a temporal datastore.
   * @throws Exception
   */
  public void open(CConfiguration configuration) throws Exception;

  /**
   * Puts a {@link DataPoint} into the temporal data store.
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
  public ImmutableList<DataPoint> execute(Query query) throws Exception;
}
