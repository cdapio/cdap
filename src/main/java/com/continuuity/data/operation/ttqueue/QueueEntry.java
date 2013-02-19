package com.continuuity.data.operation.ttqueue;

import java.util.Map;
import java.util.Set;

public interface QueueEntry {

  public void addPartitioningKey(String key, int hash);

  public Integer getHash(String key);

  public byte[] getData();

  public void setData(byte[] data);

  public Set<String> getAllPartioningKeys();

  public Map<String, Integer> getPartioningMap();
}