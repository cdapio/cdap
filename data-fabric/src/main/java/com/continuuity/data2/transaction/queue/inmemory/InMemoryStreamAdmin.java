package com.continuuity.data2.transaction.queue.inmemory;

import com.continuuity.data2.transaction.queue.StreamAdmin;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * admin for queues in memory.
 */
@Singleton
public class InMemoryStreamAdmin extends InMemoryQueueAdmin implements StreamAdmin {

  @Inject
  public InMemoryStreamAdmin(InMemoryQueueService queueService) {
    super(queueService);
  }

  @Override
  public void dropAll() throws Exception {
    queueService.resetStreams();
  }
}
