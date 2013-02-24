package com.continuuity.data.operation.ttqueue;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

public class QueueEntryImpl implements QueueEntry {
  private final Map<String, Integer> header;
  private byte[] data;

  public QueueEntryImpl(Map<String, Integer> header, byte[] data) {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(header);
    this.data=data;
    this.header=header;
  }

  public QueueEntryImpl(byte[] data) {
    Preconditions.checkNotNull(data);
    this.header= Maps.newHashMap();
    this.data=data;
  }

  @Override
  public byte[] getData() {
    return this.data;
  }

  @Override
  public void setData(byte[] data) {
    Preconditions.checkNotNull(data);
    this.data=data;
  }

  @Override
  public Map<String, Integer> getPartitioningMap() {
    return header;
  }

  protected Map<String, Integer> getHeader() {
    return this.header;
  }

  @Override
  public void addPartitioningKey(String key, int hash) {
    this.header.put(key, hash);
  }

  @Override
  public Integer getHash(String key) {
    if (header==null) {
      return null;
    }
    return this.header.get(key);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("data", this.data)
        .add("header", this.header)
        .toString();
  }
}