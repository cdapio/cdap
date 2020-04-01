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
import io.cdap.cdap.internal.app.runtime.distributed.ProgramTwillApplication;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteExecutionTwillRunnerService;
import io.cdap.cdap.internal.app.runtime.distributed.runtimejob.RuntimeJobTwillRunnerService;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
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
import org.apache.twill.internal.SingleRunnableApplication;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * A twill service that delegates calls to underlying twill runner service {@link RemoteExecutionTwillRunnerService}
 * or {@link RuntimeJobTwillRunnerService}.
 * This is need to support program runs using ssh and runtime job manager apis.
 */
public class RuntimeTwillRunnerService implements TwillRunnerService {
  private final TwillRunnerService remoteExecutionTwillService;
  private final TwillRunnerService runtimeJobTwillService;
  private final ProvisioningService provisioningService;

  @Inject
  public RuntimeTwillRunnerService(RemoteExecutionTwillRunnerService remoteExecutionTwillService,
                                   RuntimeJobTwillRunnerService runtimeJobTwillService,
                                   ProvisioningService provisioningService) {
    this.remoteExecutionTwillService = remoteExecutionTwillService;
    this.runtimeJobTwillService = runtimeJobTwillService;
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
    Optional<RuntimeJobManager> runtimeJobManager =
      provisioningService.getRuntimeJobManager(programTwillApp.getProgramOptions(), programTwillApp.getProgramRunId());
    if (runtimeJobManager.isPresent()) {
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
    return Iterables.concat(runtimeJobTwillService.lookup(applicationName),
                            remoteExecutionTwillService.lookup(applicationName));
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    return Iterables.concat(runtimeJobTwillService.lookupLive(), remoteExecutionTwillService.lookupLive());
  }

  @SuppressWarnings("deprecation")
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
