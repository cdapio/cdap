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

package io.cdap.cdap.internal.app.worker.system;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.guice.ImpersonatedTwillRunnerService;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.security.TokenSecureStoreRenewer;
import io.cdap.cdap.security.impersonation.Impersonator;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * RemoteTwillModule provides ImpersonatedTwillRunnerService which has RemoteExecutionTwillRunnerService as its
 * delegate.
 */
public class RemoteTwillModule extends AbstractModule {

  private static ClusterMode clusterMode;

  public RemoteTwillModule(ClusterMode mode) {
    clusterMode = mode;
  }

  @Override
  protected void configure() {
    Key<TwillRunnerService> twillRunnerServiceKey = Key.get(TwillRunnerService.class,
                                                            Constants.AppFabric.ProgramRunner.class);
    bind(twillRunnerServiceKey).toProvider(TwillRunnerServiceProvider.class).in(Scopes.SINGLETON);
    bind(TwillRunner.class).to(twillRunnerServiceKey);
    bind(TwillRunnerService.class).to(twillRunnerServiceKey);
  }

  static final class TwillRunnerServiceProvider implements Provider<TwillRunnerService> {

    private final Configuration hConf;
    private final Impersonator impersonator;
    private final TokenSecureStoreRenewer secureStoreRenewer;
    private final TwillRunnerService remoteExecutionTwillRunnerService;

    @Inject
    TwillRunnerServiceProvider(Configuration hConf, Impersonator impersonator,
                               TokenSecureStoreRenewer secureStoreRenewer,
                               @Constants.AppFabric.RemoteExecution TwillRunnerService
                                 remoteExecutionTwillRunnerService) {
      this.hConf = hConf;
      this.impersonator = impersonator;
      this.secureStoreRenewer = secureStoreRenewer;
      this.remoteExecutionTwillRunnerService = remoteExecutionTwillRunnerService;
    }

    @Override
    public TwillRunnerService get() {
      TwillRunnerService twillRunnerService;
      if (ClusterMode.ON_PREMISE.equals(clusterMode)) {
        MasterEnvironment masterEnv = MasterEnvironments.getMasterEnvironment();
        twillRunnerService = Optional.ofNullable(masterEnv).map(MasterEnvironment::getTwillRunnerSupplier)
          .map(Supplier::get)
          .orElseThrow(() -> new UnsupportedOperationException("TwillRunnerService not found for this clusterMode"));
      } else {
        twillRunnerService = remoteExecutionTwillRunnerService;
      }
      return new ImpersonatedTwillRunnerService(hConf, twillRunnerService, impersonator, secureStoreRenewer);
    }
  }
}
