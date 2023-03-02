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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import com.google.inject.Inject;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.auth.AccessTokenCodec;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;

/**
 * A {@link TwillRunnerService} implementations that only launches (i.e., fire and forget) {@link
 * TwillApplication} without monitoring.
 */
public class FireAndForgetTwillRunnerService extends RemoteExecutionTwillRunnerService {

  @Inject
  FireAndForgetTwillRunnerService(CConfiguration cConf, Configuration hConf,
      DiscoveryServiceClient discoveryServiceClient, LocationFactory locationFactory,
      ProvisioningService provisioningService, ProgramStateWriter programStateWriter,
      TransactionRunner transactionRunner, AccessTokenCodec accessTokenCodec,
      TokenManager tokenManager) {
    super(cConf, hConf, discoveryServiceClient, locationFactory, provisioningService,
        programStateWriter,
        transactionRunner, accessTokenCodec, tokenManager);
  }

  @Override
  public void start() {
    //  no need to initialize controllers as this is a fire and forget twill runner service.
    doInitialize();
  }

  @Nullable
  @Override
  public TwillController lookup(String applicationName, RunId runId) {
    throw new UnsupportedOperationException(
        "The lookup method is not supported in FireAndForgetTwillRunnerService.");
  }

  @Override
  public Iterable<TwillController> lookup(String applicationName) {
    throw new UnsupportedOperationException(
        "The lookup method is not supported in FireAndForgetTwillRunnerService.");
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    throw new UnsupportedOperationException("The lookupLive method is not supported in "
        + "FireAndForgetTwillRunnerService.");
  }

  @Override
  protected RemoteExecutionTwillController getController(ProgramRunId programRunId) {
    //no op as this is a fire and forget twill runner service
    return null;
  }

  @Override
  protected void addController(ProgramRunId programRunId,
      RemoteExecutionTwillController controller) {
    //no op as this is a fire and forget twill runner service
  }

  @Override
  protected void monitorController(ProgramRunId programRunId,
      CompletableFuture<Void> startupTaskCompletion,
      RemoteExecutionTwillController controller,
      RemoteExecutionService remoteExecutionService) {

    //no need to monitor the controller as this is a fire and forget twill runner service.
    controller.release();
  }

}
