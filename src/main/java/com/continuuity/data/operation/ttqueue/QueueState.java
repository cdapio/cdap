package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.operation.ttqueue.internal.CachedList;

/**
 *
 */
public interface QueueState {

  public long getActiveEntryId();

  public void setActiveEntryId(long activeEntryId);

  public long getConsumerReadPointer();

  public void setConsumerReadPointer(long consumerReadPointer);

  public CachedList<QueueStateEntry> getCachedEntries();

}
