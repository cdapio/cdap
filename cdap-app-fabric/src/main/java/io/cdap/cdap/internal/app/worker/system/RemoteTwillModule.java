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
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.security.TokenSecureStoreRenewer;
import io.cdap.cdap.security.impersonation.Impersonator;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RemoteTwillModule provides ImpersonatedTwillRunnerService which has RemoteExecutionTwillRunnerService as its
 * delegate.
 */
public class RemoteTwillModule extends AbstractModule {

  private static final Logger LOG = LoggerFactory.getLogger(SystemWorkerTwillRunnable.class);

  @Override
  protected void configure() {
    Key<TwillRunnerService> twillRunnerServiceKey = Key.get(TwillRunnerService.class,
                                                            Constants.AppFabric.ProgramRunner.class);
    bind(twillRunnerServiceKey).toProvider(TwillRunnerServiceProvider.class).in(Scopes.SINGLETON);
//    bind(TwillRunner.class).to(twillRunnerServiceKey);
//    bind(TwillRunnerService.class).to(twillRunnerServiceKey);
  }

  static final class TwillRunnerServiceProvider implements Provider<TwillRunnerService> {

    private final Configuration hConf;
    private final Impersonator impersonator;
    private final TokenSecureStoreRenewer secureStoreRenewer;
    private final TwillRunnerService remoteExecutionTwillRunnerService;
    private final TwillRunnerService twillRunnerService;

    @Inject
    TwillRunnerServiceProvider(Configuration hConf, Impersonator impersonator,
                               TokenSecureStoreRenewer secureStoreRenewer,
                               @Constants.AppFabric.RemoteExecution TwillRunnerService
                                 remoteExecutionTwillRunnerService,
                               TwillRunnerService twillRunnerService) {
      this.hConf = hConf;
      this.impersonator = impersonator;
      this.secureStoreRenewer = secureStoreRenewer;
      this.remoteExecutionTwillRunnerService = remoteExecutionTwillRunnerService;
      this.twillRunnerService = twillRunnerService;
      LOG.debug("RETRS: {}", remoteExecutionTwillRunnerService);
      LOG.debug("TRS: {}", twillRunnerService);
    }

    @Override
    public TwillRunnerService get() {
      return twillRunnerService;
//      return new ImpersonatedTwillRunnerService(hConf, twillRunnerService,
//                                                impersonator, secureStoreRenewer);
    }
  }
}
