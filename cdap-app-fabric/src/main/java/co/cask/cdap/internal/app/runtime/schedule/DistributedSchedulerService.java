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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.config.PreferencesStore;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import org.quartz.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Scheduler service to run in Distributed CDAP. Waits for Dataset service to be available.
 */
public final class DistributedSchedulerService extends AbstractSchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedSchedulerService.class);
  private Thread startSchedulerThread;

  @Inject
  public DistributedSchedulerService(Supplier<Scheduler> schedulerSupplier,
                                     StreamSizeScheduler streamSizeScheduler, StoreFactory storeFactory,
                                     ProgramRuntimeService programRuntimeService,
                                     PreferencesStore preferencesStore, CConfiguration cConf) {
    super(schedulerSupplier, streamSizeScheduler, storeFactory, programRuntimeService, preferencesStore, cConf);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting scheduler.");
    startSchedulerThread = new Thread("Scheduler-Start-Up") {
      @Override
      public void run() {
        boolean started = false;
        while (!started && !isInterrupted()) {
          try {
            startSchedulers();
            started = true;
            LOG.info("Scheduler started successfully.");
          } catch (Throwable t) {
            LOG.error("Error starting scheduler {}", t.getMessage());
            try {
              TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException ie) {
              break;
            }
          }
        }
      }
    };
    startSchedulerThread.start();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping scheduler.");
    startSchedulerThread.interrupt();
    startSchedulerThread.join();
    stopScheduler();
  }
}
