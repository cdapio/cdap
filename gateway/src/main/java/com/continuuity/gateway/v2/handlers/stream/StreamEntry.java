package com.continuuity.gateway.v2.handlers.stream;

import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.google.common.util.concurrent.FutureCallback;

/**
 * Represents a stream entry with a callback to be called after enqueuing the entry.
 */
class StreamEntry {
  private final QueueEntry queueEntry;
  private final FutureCallback<Void> callback;

  StreamEntry(QueueEntry queueEntry, FutureCallback<Void> callback) {
    this.queueEntry = queueEntry;
    this.callback = callback;
  }

  public QueueEntry getQueueEntry() {
    return queueEntry;
  }

  public FutureCallback<Void> getCallback() {
    return callback;
  }
}
