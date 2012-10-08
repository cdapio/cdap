package com.continuuity.metrics2.common;

import com.google.common.collect.ImmutableList;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: nmotgi
 * Date: 10/7/12
 * Time: 5:45 PM
 * To change this template use File | Settings | File Templates.
 */
public interface DataStore {
  public void put(DataPoint point) throws Exception;
  public ImmutableList<DataPoint> getDataPoints(Query query) throws Exception;
}
