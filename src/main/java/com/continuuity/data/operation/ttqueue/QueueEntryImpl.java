package com.continuuity.data.operation.ttqueue;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

import java.util.Map;

public class QueueEntryImpl implements QueueEntry {
  private final Map<String, Integer> header;
  private byte[] data;

  public QueueEntryImpl(Map<String, Integer> header, byte[] data) {
    this.header=header;
    this.data=data;
  }

  public QueueEntryImpl(byte[] data) {
    this.header=Maps.newHashMap();
    this.data=data;
  }

  public byte[] getData() {
    return this.data;
  }

  @Override
  public void setData(byte[] data) {
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