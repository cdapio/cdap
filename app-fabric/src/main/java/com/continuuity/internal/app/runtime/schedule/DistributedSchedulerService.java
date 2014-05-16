package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.Constants;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Scheduler service to run in distributed reactor. Waits for transaction service to be available.
 */
public final class DistributedSchedulerService extends DefaultSchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedSchedulerService.class);
  private final DiscoveryServiceClient discoveryServiceClient;
  private final ListeningExecutorService executorService;
  private final AtomicBoolean schedulerStarted = new AtomicBoolean(false);
  private Cancellable cancellable;

  @Inject
  public DistributedSchedulerService(Supplier<Scheduler> schedulerSupplier, StoreFactory storeFactory,
                                     ProgramRuntimeService programRuntimeService,
                                     DiscoveryServiceClient discoveryServiceClient) {
    super(schedulerSupplier, storeFactory, programRuntimeService);
    this.discoveryServiceClient = discoveryServiceClient;
    executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
  }

  @Override
  protected void startScheduler(final WrappedScheduler scheduler) {
    //Wait till TransactionService is discovered then start the scheduler.
    ServiceDiscovered discover = discoveryServiceClient.discover(Constants.Service.TRANSACTION);
    cancellable = discover.watchChanges(
      new ServiceDiscovered.ChangeListener() {
      @Override
      public void onChange(ServiceDiscovered serviceDiscovered) {
        if (!Iterables.isEmpty(serviceDiscovered) && !schedulerStarted.get()) {
            LOG.info("Starting scheduler, Discovered {} transaction service(s)",
                      Iterables.size(serviceDiscovered));
          try {
            scheduler.start();
            schedulerStarted.set(true);
          } catch (SchedulerException e) {
            LOG.error("Error starting scheduler {}", e.getCause(), e);
            throw Throwables.propagate(e);
          }
        }
      }
    }, MoreExecutors.sameThreadExecutor());
  }

  @Override
  protected void stopScheduler(WrappedScheduler scheduler) {
    try {
      LOG.info("Stopping scheduler");
      scheduler.stop();
    } catch (SchedulerException e) {
      LOG.debug("Error stopping scheduler {}", e.getCause(), e);
    } finally {
      schedulerStarted.set(false);
      if (cancellable != null) {
        cancellable.cancel();
      }
    }
  }
}
