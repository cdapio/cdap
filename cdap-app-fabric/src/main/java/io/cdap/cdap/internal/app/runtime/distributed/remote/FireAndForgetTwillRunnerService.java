/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

import com.google.inject.Inject;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.auth.AccessTokenCodec;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;

/**
 * This class extends RemoteExecutionTwillRunnerService with fire and forget capability in which a twillrunner
 * is solely launched, but it is not monitored for running/termination.
 */
public class FireAndForgetTwillRunnerService extends RemoteExecutionTwillRunnerService {

  private final CConfiguration cConf;

  @Inject
  FireAndForgetTwillRunnerService(CConfiguration cConf, Configuration hConf,
                                  DiscoveryServiceClient discoveryServiceClient, LocationFactory locationFactory,
                                  ProvisioningService provisioningService, ProgramStateWriter programStateWriter,
                                  TransactionRunner transactionRunner, AccessTokenCodec accessTokenCodec,
                                  TokenManager tokenManager) {
    super(cConf, hConf, discoveryServiceClient,
          locationFactory, provisioningService, programStateWriter,
          transactionRunner, accessTokenCodec, tokenManager);
    this.cConf = cConf;
  }

  @Override
  public void start() {
    //No need to call super start since super also initialize (existing) controllers.
    super.doInitialize();
  }

  @Override
  public TwillController lookup(String applicationName, RunId runId) {
    throw new UnsupportedOperationException("The lookup method is not supported in FireAndForgetTwillRunnerService");
  }

  @Override
  public Iterable<TwillController> lookup(String applicationName) {
    throw new UnsupportedOperationException("The lookup method is not supported in FireAndForgetTwillRunnerService");
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    throw new
      UnsupportedOperationException("The lookupLive method is not supported in FireAndForgetTwillRunnerService");
  }

  @Override
  protected TwillControllerFactory createControllerFactory(
    RemoteExecutionTwillRunnerService remoteExecutionTwillRunnerService, ProgramRunId programRunId,
    ProgramOptions programOpts) {

    return new FireAndForgetControllerFactory(this, programRunId, programOpts);
  }

  @Override
  void addController(ProgramRunId programRunId, RemoteExecutionTwillController controller) {
    //no op
  }

  @Override
  boolean removeController(ProgramRunId programRunId, RemoteExecutionTwillController controller) {
    return true;
    //no op
  }
}
