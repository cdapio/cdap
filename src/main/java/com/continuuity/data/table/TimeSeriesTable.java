package com.continuuity.data.table;

import java.util.Map;

public interface TimeSeriesTable {

  public void addPoint(byte [] key, long time, byte [] value);
  
  public byte [] getPoint(byte [] key, long time);
  
  public Map<Long,byte[]> getPoints(byte [] key, long startTime, long endTime);

}
