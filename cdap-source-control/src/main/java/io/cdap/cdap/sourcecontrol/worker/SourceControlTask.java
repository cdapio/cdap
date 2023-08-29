/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.sourcecontrol.worker;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.internal.remote.RunnableTaskModule;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.guice.SecureStoreClientModule;
import io.cdap.cdap.sourcecontrol.operationrunner.InMemorySourceControlOperationRunner;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * The abstract class that creates the {@link InMemorySourceControlOperationRunner}.
 */
abstract class SourceControlTask implements RunnableTask {

  protected final InMemorySourceControlOperationRunner inMemoryOperationRunner;

  SourceControlTask(CConfiguration cConf,
      DiscoveryService discoveryService,
      DiscoveryServiceClient discoveryServiceClient,
      MetricsCollectionService metricsCollectionService) {
    Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        RemoteAuthenticatorModules.getDefaultModule(),
        new SecureStoreClientModule(),
        new AuthenticationContextModules().getMasterWorkerModule(),
        new RunnableTaskModule(discoveryService, discoveryServiceClient,
            metricsCollectionService)
    );
    inMemoryOperationRunner = injector.getInstance(
        InMemorySourceControlOperationRunner.class);
  }

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    inMemoryOperationRunner.startAndWait();
    doRun(context);
  }

  protected abstract void doRun(RunnableTaskContext context) throws Exception;

}
