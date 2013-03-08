package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.operation.ttqueue.internal.CachedList;

/**
 *
 */
public interface QueueState {

  public long getActiveEntryId();

  public void setActiveEntryId(long activeEntryId);

  public CachedList<Integer> getCachedHeaders();

  public CachedList<byte[]> getCachedEntries();

}
