package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.operation.ttqueue.internal.CachedList;

/**
 *
 */
public class QueueStateImpl implements QueueState {
  private long activeEntryId;
  private final CachedList<Integer> cachedHeaders;
  private final CachedList<byte[]> cachedEntries;

  public QueueStateImpl(CachedList<Integer> cachedHeaders, CachedList<byte[]> cachedEntries) {
    this.cachedHeaders = cachedHeaders;
    this.cachedEntries = cachedEntries;
  }

  public QueueStateImpl(CachedList<byte[]> cachedEntries) {
    this.cachedHeaders = null;
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
  public CachedList<Integer> getCachedHeaders() {
    return cachedHeaders;
  }

  @Override
  public CachedList<byte[]> getCachedEntries() {
    return cachedEntries;
  }
}
