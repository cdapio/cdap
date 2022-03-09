/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import io.cdap.cdap.app.guice.ImpersonatedTwillRunnerService;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteExecutionTwillRunnerService;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.security.TokenSecureStoreRenewer;
import io.cdap.cdap.security.auth.AccessTokenCodec;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteTwillModule extends AbstractModule {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteTwillModule.class);

  @Override
  protected void configure() {
    Key<TwillRunnerService> twillRunnerServiceKey = Key.get(TwillRunnerService.class,
        Constants.AppFabric.ProgramRunner.class);
    bind(twillRunnerServiceKey).toProvider(TwillRunnerServiceProvider.class).in(Scopes.SINGLETON);
    bind(TwillRunner.class).to(twillRunnerServiceKey);
    bind(TwillRunnerService.class).annotatedWith(Constants.AppFabric.RemoteExecution.class)
        .to(twillRunnerServiceKey);
  }

  static final class TwillRunnerServiceProvider implements Provider<TwillRunnerService> {

    private final CConfiguration cConf;
    private final Configuration hConf;
    private final LocationFactory locationFactory;
    private final Impersonator impersonator;
    private final TokenSecureStoreRenewer secureStoreRenewer;
    private final ProvisioningService provisioningService;
    private final DiscoveryServiceClient discoveryServiceClient;
    private final ProgramStateWriter programStateWriter;
    private final TransactionRunner transactionRunner;
    private final AccessTokenCodec accessTokenCodec;
    private final TokenManager tokenManager;

    @Inject
    TwillRunnerServiceProvider(CConfiguration cConf, Configuration hConf, Impersonator impersonator,
        LocationFactory locationFactory, TokenSecureStoreRenewer secureStoreRenewer,
        ProvisioningService provisioningService, DiscoveryServiceClient discoveryServiceClient,
        ProgramStateWriter programStateWriter, TransactionRunner transactionRunner,
        AccessTokenCodec accessTokenCodec, TokenManager tokenManager) {
      this.cConf = cConf;
      this.hConf = hConf;
      this.locationFactory = locationFactory;
      this.impersonator = impersonator;
      this.secureStoreRenewer = secureStoreRenewer;
      this.provisioningService = provisioningService;
      this.discoveryServiceClient = discoveryServiceClient;
      this.programStateWriter = programStateWriter;
      this.transactionRunner = transactionRunner;
      this.accessTokenCodec = accessTokenCodec;
      this.tokenManager = tokenManager;
      LOG.debug("ProvisioningService in RemoteTwillModule: {}", provisioningService.toString());
    }

    @Override
    public TwillRunnerService get() {
      RemoteExecutionTwillRunnerService runner = new RemoteExecutionTwillRunnerService(cConf, hConf,
          discoveryServiceClient, locationFactory, provisioningService, programStateWriter,
          transactionRunner, accessTokenCodec, tokenManager);
      return new ImpersonatedTwillRunnerService(hConf, runner, impersonator, secureStoreRenewer);
    }
  }


}
