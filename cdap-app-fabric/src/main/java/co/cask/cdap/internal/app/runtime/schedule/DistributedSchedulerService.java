/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.config.PreferencesWrapper;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.quartz.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Scheduler service to run in Distributed CDAP. Waits for Dataset service to be available.
 */
public final class DistributedSchedulerService extends AbstractSchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedSchedulerService.class);
  private final DiscoveryServiceClient discoveryServiceClient;
  private final AtomicBoolean schedulerStarted = new AtomicBoolean(false);
  private Cancellable cancellable;

  @Inject
  public DistributedSchedulerService(Supplier<Scheduler> schedulerSupplier, StoreFactory storeFactory,
                                     ProgramRuntimeService programRuntimeService,
                                     DiscoveryServiceClient discoveryServiceClient,
                                     PreferencesWrapper preferencesWrapper) {
    super(schedulerSupplier, storeFactory, programRuntimeService, preferencesWrapper);
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  protected void startUp() throws Exception {
    // Wait till DatasetService is discovered then start the scheduler.
    ServiceDiscovered discover = discoveryServiceClient.discover(Constants.Service.DATASET_MANAGER);
    cancellable = discover.watchChanges(
      new ServiceDiscovered.ChangeListener() {
        @Override
        public void onChange(ServiceDiscovered serviceDiscovered) {
          if (!Iterables.isEmpty(serviceDiscovered) && !schedulerStarted.get()) {
            LOG.info("Starting scheduler, Discovered {} dataset service(s)",
                     Iterables.size(serviceDiscovered));
            try {
              startScheduler();
              schedulerStarted.set(true);
            } catch (Throwable t) {
              LOG.error("Exception when starting scheduler.", t);
            }
          }
        }
      }, MoreExecutors.sameThreadExecutor());
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      LOG.info("Stopping scheduler");
      stopScheduler();
    } finally {
      schedulerStarted.set(false);
      if (cancellable != null) {
        cancellable.cancel();
      }
    }
  }
}
