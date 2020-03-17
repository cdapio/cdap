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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.monitor.RemoteExecutionLogProcessor;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.messaging.MessagingService;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.inject.Inject;

/**
 *
 */
public class ForwardingTwillRunnerService implements TwillRunnerService {
  private static final Logger LOG = LoggerFactory.getLogger(ForwardingTwillRunnerService.class);



  @Inject
  public ForwardingTwillRunnerService(CConfiguration cConf, Configuration hConf,
                                      DiscoveryServiceClient discoveryServiceClient,
                                      LocationFactory locationFactory, MessagingService messagingService,
                                      RemoteExecutionLogProcessor logProcessor,
                                      MetricsCollectionService metricsCollectionService,
                                      ProvisioningService provisioningService, ProgramStateWriter programStateWriter,
                                      TransactionRunner transactionRunner) {


  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {

  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable) {
    return null;
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable, ResourceSpecification resourceSpecification) {
    return null;
  }

  @Override
  public TwillPreparer prepare(TwillApplication application) {
    return null;
  }

  @Override
  public TwillController lookup(String applicationName, RunId runId) {
    return null;
  }

  @Override
  public Iterable<TwillController> lookup(String applicationName) {
    return null;
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    return null;
  }

  @Override
  public Cancellable scheduleSecureStoreUpdate(SecureStoreUpdater updater, long initialDelay, long delay, TimeUnit unit) {
    return null;
  }

  @Override
  public Cancellable setSecureStoreRenewer(SecureStoreRenewer renewer, long initialDelay, long delay, long retryDelay, TimeUnit unit) {
    return null;
  }
}
