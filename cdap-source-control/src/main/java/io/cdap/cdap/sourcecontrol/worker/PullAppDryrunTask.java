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

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.ReadonlyArtifactRepositoryAccessor;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.proto.sourcecontrol.PullAppDryrunResponse;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.operationrunner.PulAppOperationRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.PullAndDryrunAppOperationRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.PullAppResponse;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import javafx.util.Pair;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link io.cdap.cdap.api.service.worker.RunnableTask} to pull an application
 * from a remote Git repository and dryrun the deployment
 */
public class PullAppDryrunTask extends SourceControlTask {
  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(PullAppTask.class);

  @Inject
  PullAppDryrunTask(CConfiguration cConf,
      DiscoveryService discoveryService,
      DiscoveryServiceClient discoveryServiceClient,
      MetricsCollectionService metricsCollectionService) {
    super(cConf, discoveryService, discoveryServiceClient,
        metricsCollectionService);
  }

  @Override
  public void doRun(RunnableTaskContext context)
      throws AuthenticationConfigException, IOException, NotFoundException, Exception {
    Type paramsType = new TypeToken<Pair<PullAndDryrunAppOperationRequest, ReadonlyArtifactRepositoryAccessor>>(){}.getType();
    Pair<PullAndDryrunAppOperationRequest, ReadonlyArtifactRepositoryAccessor> params = GSON.fromJson(context.getParam(), paramsType);

    LOG.info("Pulling application {} in worker.", params.getKey().getApp().getApplication());
    PullAppDryrunResponse result = inMemoryOperationRunner.pullAndDryrun(params.getKey(), params.getValue());
    context.writeResult(GSON.toJson(result).getBytes(StandardCharsets.UTF_8));
  }
}
