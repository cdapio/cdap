package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.operation.ttqueue.internal.CachedList;
import java.util.Collections;

/**
 *
 */
public class QueueStateImpl implements QueueState {
  private long activeEntryId;
  private long consumerReadPointer;
  private final CachedList<QueueStateEntry> cachedEntries;

  public QueueStateImpl() {
    activeEntryId = QueueState.INVALID_ACTIVE_ENTRY_ID;
    cachedEntries = new CachedList<QueueStateEntry>(Collections.EMPTY_LIST);
  }
  public QueueStateImpl(CachedList<QueueStateEntry> cachedEntries) {
    this.cachedEntries = cachedEntries;
  }

  @Override
  public long getActiveEntryId() {
    return activeEntryId;
  }

  @Override
  public void setActiveEntryId(long activeEntryId) {
    this.activeEntryId = activeEntryId;
  }

  @Override
  public long getConsumerReadPointer() {
    return consumerReadPointer;
  }

  @Override
  public void setConsumerReadPointer(long consumerReadPointer) {
    this.consumerReadPointer = consumerReadPointer;
  }

  @Override
  public CachedList<QueueStateEntry> getCachedEntries() {
    return cachedEntries;
  }
}
