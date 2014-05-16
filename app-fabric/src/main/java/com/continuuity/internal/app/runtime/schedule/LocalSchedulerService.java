package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.store.StoreFactory;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import org.quartz.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LocalSchedulerService - noop for pre and post hooks.
 */
public final class LocalSchedulerService extends AbstractSchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(LocalSchedulerService.class);

  @Inject
  public LocalSchedulerService(Supplier<Scheduler> schedulerSupplier, StoreFactory storeFactory,
                               ProgramRuntimeService programRuntimeService) {
    super(schedulerSupplier, storeFactory, programRuntimeService);
  }

  @Override
  protected void startUp() throws Exception {
    startScheduler();
  }

  @Override
  protected void shutDown() throws Exception {
    stopScheduler();
  }
}
