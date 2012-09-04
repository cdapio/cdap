package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;

import java.util.Map;

public interface TimeSeriesTable {

  public void addPoint(byte [] key, long time, byte [] value) throws OperationException;
  
  public OperationResult<byte[]> getPoint(byte[] key, long time) throws OperationException;
  
  public Map<Long,byte[]> getPoints(byte [] key, long startTime, long endTime) throws OperationException;

}
