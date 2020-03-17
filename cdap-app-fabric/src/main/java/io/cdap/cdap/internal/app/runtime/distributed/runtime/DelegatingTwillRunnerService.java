/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.runtime;

import com.google.common.collect.Iterables;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.distributed.ProgramTwillApplication;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteExecutionTwillRunnerService;
import io.cdap.cdap.internal.app.runtime.distributed.runtimejob.RuntimeJobTwillRunnerService;
import io.cdap.cdap.internal.app.runtime.monitor.RemoteExecutionLogProcessor;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.security.SecureStoreRenewer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.SingleRunnableApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * Delegates call to twill runner service methods to {@link RemoteExecutionTwillRunnerService} or
 * {@link RuntimeJobTwillRunnerService}.
 */
  public class DelegatingTwillRunnerService implements TwillRunnerService {
  private static final Logger LOG = LoggerFactory.getLogger(DelegatingTwillRunnerService.class);

  private final TwillRunnerService remoteExecutionTwillService;
  private final TwillRunnerService runtimeJobTwillService;
  private final ProvisioningService provisioningService;

  @Inject
  public DelegatingTwillRunnerService(CConfiguration cConf, Configuration hConf,
                                      DiscoveryServiceClient discoveryServiceClient,
                                      LocationFactory locationFactory, MessagingService messagingService,
                                      RemoteExecutionLogProcessor logProcessor,
                                      MetricsCollectionService metricsCollectionService,
                                      ProvisioningService provisioningService,
                                      ProgramStateWriter programStateWriter,
                                      TransactionRunner transactionRunner) {
    this.remoteExecutionTwillService = new RemoteExecutionTwillRunnerService(cConf, hConf, discoveryServiceClient,
                                                                             locationFactory, messagingService,
                                                                             logProcessor, metricsCollectionService,
                                                                             provisioningService, programStateWriter,
                                                                             transactionRunner);
    this.runtimeJobTwillService = new RuntimeJobTwillRunnerService(cConf, hConf, locationFactory,
                                                                   provisioningService, programStateWriter);
    this.provisioningService = provisioningService;
  }


  @Override
  public void start() {
    remoteExecutionTwillService.start();
    runtimeJobTwillService.start();
  }

  @Override
  public void stop() {
    remoteExecutionTwillService.stop();
    runtimeJobTwillService.stop();
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable) {
    return prepare(runnable, ResourceSpecification.BASIC);
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable, ResourceSpecification resourceSpecification) {
    return prepare(new SingleRunnableApplication(runnable, resourceSpecification));
  }

  @Override
  public TwillPreparer prepare(TwillApplication application) {
    // Restrict the usage to launch user program only.
    if (!(application instanceof ProgramTwillApplication)) {
      throw new IllegalArgumentException("Only instance of ProgramTwillApplication is supported.");
    }

    ProgramTwillApplication programTwillApp = (ProgramTwillApplication) application;
    ProgramRunId programRunId = programTwillApp.getProgramRunId();
    ProgramOptions programOptions = programTwillApp.getProgramOptions();
    Optional<RuntimeJobManager> runtimeJobManager =
      provisioningService.getRuntimeJobManager(programOptions, programRunId);
    if (runtimeJobManager.isPresent()) {
      LOG.info("### Using runtimeJobTwillService....");
      return runtimeJobTwillService.prepare(application);
    }
    return remoteExecutionTwillService.prepare(application);
  }

  @Override
  @Nullable
  public TwillController lookup(String applicationName, RunId runId) {
    TwillController controller = runtimeJobTwillService.lookup(applicationName, runId);
    if (controller != null) {
      return controller;
    }
    return remoteExecutionTwillService.lookup(applicationName, runId);
  }

  @Override
  public Iterable<TwillController> lookup(String applicationName) {
    Iterable<TwillController> runtimeJobControllers = runtimeJobTwillService.lookup(applicationName);
    Iterable<TwillController> remoteExecutionControllers = remoteExecutionTwillService.lookup(applicationName);

    return Iterables.concat(runtimeJobControllers, remoteExecutionControllers);
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    Iterable<LiveInfo> runtimeJobLiveInfos = runtimeJobTwillService.lookupLive();
    Iterable<LiveInfo> remoteExecutionLiveInfos = remoteExecutionTwillService.lookupLive();
    return Iterables.concat(runtimeJobLiveInfos, remoteExecutionLiveInfos);
  }

  @Override
  public Cancellable scheduleSecureStoreUpdate(SecureStoreUpdater updater, long initialDelay,
                                               long delay, TimeUnit unit) {
    // This method is deprecated and not used in CDAP
    throw new UnsupportedOperationException("The scheduleSecureStoreUpdate method is deprecated, " +
                                              "use setSecureStoreRenewer instead");
  }

  @Override
  public Cancellable setSecureStoreRenewer(SecureStoreRenewer renewer, long initialDelay, long delay,
                                           long retryDelay, TimeUnit unit) {
    return () -> { };
  }
}
