package com.continuuity.data2.transaction.queue.inmemory;

import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 *
 */
@Singleton
public class InMemoryQueueAdmin implements QueueAdmin {
  private final InMemoryQueueService queueService;

  @Inject
  public InMemoryQueueAdmin(InMemoryQueueService queueService) {
    this.queueService = queueService;
  }

  @Override
  public boolean exists(String name) throws Exception {
    // no special actions needed to create it
    return true;
  }

  @Override
  public void create(String name) throws Exception {
    // do nothing
  }

  @Override
  public void truncate(String name) throws Exception {
    queueService.truncate(name);
  }

  @Override
  public void drop(String name) throws Exception {
    queueService.drop(name);
  }

  @Override
  public void dropAll() throws Exception {
    queueService.reset();
  }
}
