package com.continuuity.data.stream;

import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 *  Represents entry in the stream - has header and data. This is implemented based on QueueEntry since the
 *  implementation of Streams is done using queues. To genericize later.
 */
public class StreamEntry {
  private final Map<String, Integer> header;
  private final byte[] data;

  public StreamEntry(Map<String, Integer> header, byte[] data) {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(header);
    this.header = header;
    this.data = data;
  }

  public Map<String, Integer> getHeader() {
    return header;
  }

  public byte[] getData() {
    return data;
  }

  public StreamEntry( byte[] data) {
    Preconditions.checkNotNull(data);
    this.header = Maps.newHashMap();
    this.data = data;
  }

  public QueueEntry toQueueEntry(){
    return new QueueEntry(header,data);
  }

  public static StreamEntry fromQueueEntry(QueueEntry entry) {
    return new StreamEntry(entry.getHashKeys(), entry.getData());
  }

}
