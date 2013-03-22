package com.continuuity.data.operation.ttqueue;

/**
*
*/
public class QueueStateEntry {
  private final byte[] data;
  private final long entryId;

  public QueueStateEntry(byte[] data, long entryId) {
    this.data = data;
    this.entryId = entryId;
  }

  public byte[] getData() {
    return data;
  }

  public long getEntryId() {
    return entryId;
  }
}
