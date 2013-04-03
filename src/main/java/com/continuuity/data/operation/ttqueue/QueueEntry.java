package com.continuuity.data.operation.ttqueue;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

public class QueueEntry {
  private final Map<String, Integer> header;
  private byte[] data;

  public QueueEntry(Map<String, Integer> header, byte[] data) {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(header);
    this.data=data;
    this.header=header;
  }

  public QueueEntry(byte[] data) {
    Preconditions.checkNotNull(data);
    this.header= Maps.newHashMap();
    this.data=data;
  }

  public byte[] getData() {
    return this.data;
  }

  public void setData(byte[] data) {
    Preconditions.checkNotNull(data);
    this.data=data;
  }

  public Map<String, Integer> getPartitioningMap() {
    return header;
  }

  public Map<String, Integer> getHeader() {
    return this.header;
  }

  public void addPartitioningKey(String key, int hash) {
    this.header.put(key, hash);
  }

  public Integer getHash(String key) {
    if (header==null) {
      return null;
    }
    return this.header.get(key);
  }

  public String toString() {
    return Objects.toStringHelper(this)
      .add("data", this.data)
      .add("header", this.header)
      .toString();
  }
}