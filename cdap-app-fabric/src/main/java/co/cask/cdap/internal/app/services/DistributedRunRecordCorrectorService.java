/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A distributed-mode only run record corrector service that corrects run records at a scheduled, configurable rate.
 */
public class DistributedRunRecordCorrectorService extends RunRecordCorrectorService {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedRunRecordCorrectorService.class);

  private final CConfiguration cConf;
  private ScheduledExecutorService scheduledExecutorService;

  @Inject
  DistributedRunRecordCorrectorService(CConfiguration cConf, Store store, ProgramStateWriter programStateWriter,
                                       ProgramRuntimeService runtimeService, NamespaceAdmin namespaceAdmin,
                                       DatasetFramework datasetFramework) {
    super(cConf, store, programStateWriter, runtimeService, namespaceAdmin, datasetFramework);
    this.cConf = cConf;
  }

  @Override
  protected void startUp() throws Exception {
    super.startUp();

    scheduledExecutorService = Executors.newScheduledThreadPool(1);
    long interval = cConf.getLong(Constants.AppFabric.PROGRAM_RUNID_CORRECTOR_INTERVAL_SECONDS);
    if (interval <= 0) {
      LOG.debug("Invalid run id corrector interval {}. Setting it to 180 seconds.", interval);
      interval = 180L;
    }
    // Schedule the run record corrector with 5 minutes initial delay and 180 seconds interval between runs
    scheduledExecutorService.scheduleWithFixedDelay(new RunRecordsCorrectorRunnable(),
                                                    300L, interval, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    super.shutDown();

    scheduledExecutorService.shutdown();
    try {
      if (!scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduledExecutorService.shutdownNow();
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Helper class to run in separate thread to validate the invalid running run records
   */
  private class RunRecordsCorrectorRunnable implements Runnable {

    @Override
    public void run() {
      try {
        LOG.trace("Start correcting invalid run records ...");

        // Lets update the running programs run records
        fixRunRecords();

        LOG.trace("End correcting invalid run records.");
      } catch (Throwable t) {
        // Ignore any exception thrown since this behaves like daemon thread.
        //noinspection ThrowableResultOfMethodCallIgnored
        LOG.warn("Unable to complete correcting run records: {}", Throwables.getRootCause(t).getMessage());
        LOG.debug("Exception thrown when running run id cleaner.", t);
      }
    }
  }
}
