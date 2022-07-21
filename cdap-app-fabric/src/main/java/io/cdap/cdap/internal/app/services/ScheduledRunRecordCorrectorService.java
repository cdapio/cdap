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

package io.cdap.cdap.internal.app.services;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A distributed-mode only run record corrector service that corrects run records at a scheduled, configurable rate.
 */
public class ScheduledRunRecordCorrectorService extends RunRecordCorrectorService {
  private static final Logger LOG = LoggerFactory.getLogger(ScheduledRunRecordCorrectorService.class);

  private ScheduledExecutorService scheduledExecutorService;
  private final long initialDelay;
  private final long interval;
  private final boolean runOnce;
  private boolean done;

  @Inject
  ScheduledRunRecordCorrectorService(CConfiguration cConf, Store store, ProgramStateWriter programStateWriter,
                                     ProgramRuntimeService runtimeService, NamespaceAdmin namespaceAdmin,
                                     DatasetFramework datasetFramework) {
    this(cConf, store, programStateWriter, runtimeService, namespaceAdmin, datasetFramework, 300L, null, false);
  }

  ScheduledRunRecordCorrectorService(CConfiguration cConf, Store store, ProgramStateWriter programStateWriter,
                                     ProgramRuntimeService runtimeService, NamespaceAdmin namespaceAdmin,
                                     DatasetFramework datasetFramework,
                                     long initialDelay, @Nullable Long interval, boolean runOnce) {
    super(cConf, store, programStateWriter, runtimeService, namespaceAdmin, datasetFramework);
    this.runOnce = runOnce;
    this.interval = computeInterval(interval, cConf);
    this.initialDelay = initialDelay;
  }

  private long computeInterval(@Nullable Long givenInterval, CConfiguration cConf) {
    if (givenInterval != null) {
      return givenInterval;
    }
    long configuredInterval = cConf.getLong(Constants.AppFabric.PROGRAM_RUNID_CORRECTOR_INTERVAL_SECONDS);
    if (configuredInterval <= 0) {
      LOG.debug("Invalid run id corrector interval {}. Setting it to 180 seconds.", configuredInterval);
      configuredInterval = 180L;
    }
    return configuredInterval;
  }

  @Override
  protected void startUp() throws Exception {
    super.startUp();

    scheduledExecutorService = Executors.newScheduledThreadPool(1, Threads.createDaemonThreadFactory("run-corrector"));
    // Schedule the run record corrector with the configured initial delay and interval between runs
    scheduledExecutorService.scheduleWithFixedDelay(new RunRecordsCorrectorRunnable(),
       initialDelay, interval, TimeUnit.SECONDS);
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
      // in case the scheduledExecutorService does not shutdown right away, avoid running repeatedly
      if (done) {
        return;
      }
      try {
        LOG.trace("Start correcting invalid run records ...");

        // Lets update the running programs run records
        fixRunRecords();

        LOG.trace("End correcting invalid run records.");

        // if configured to run only until successful once, mark success and shutdown the executor
        if (runOnce) {
          done = true;
          LOG.debug("Corrected run records successfully. Run record correction will run again when CDAP restarts.");
          scheduledExecutorService.shutdown();
        }
      } catch (Throwable t) {
        // Ignore any exception thrown since this behaves like daemon thread.
        //noinspection ThrowableResultOfMethodCallIgnored
        LOG.warn("Unable to complete correcting run records: {}", Throwables.getRootCause(t).getMessage());
        LOG.debug("Exception thrown when running run id cleaner.", t);
      }
    }
  }
}
