package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.store.StoreFactory;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LocalSchedulerService - noop for pre and post hooks.
 */
public final class LocalSchedulerService extends DefaultSchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(LocalSchedulerService.class);

  @Inject
  public LocalSchedulerService(Supplier<Scheduler> schedulerSupplier, StoreFactory storeFactory,
                               ProgramRuntimeService programRuntimeService) {
    super(schedulerSupplier, storeFactory, programRuntimeService);
  }

  @Override
  protected void startScheduler(WrappedScheduler scheduler) {
    try {
      scheduler.start();
      LOG.info("Scheduler started.");
    } catch (SchedulerException e) {
      LOG.error("Error starting scheduler {}", e.getCause(), e);
    }
  }

  @Override
  protected void stopScheduler(WrappedScheduler scheduler) {
    try {
      scheduler.stop();
      LOG.info("Scheduler stopped.");
    } catch (SchedulerException e) {
      LOG.error("Error stopping scheduler {}", e.getCause(), e);
    }
  }

}
