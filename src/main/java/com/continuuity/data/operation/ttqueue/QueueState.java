package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.operation.ttqueue.internal.CachedList;

/**
 *
 */
public interface QueueState {

  static final long INVALID_ACTIVE_ENTRY_ID = -1;

  public long getActiveEntryId();

  public void setActiveEntryId(long activeEntryId);

  public long getConsumerReadPointer();

  public void setConsumerReadPointer(long consumerReadPointer);

  public CachedList<QueueStateEntry> getCachedEntries();

}
