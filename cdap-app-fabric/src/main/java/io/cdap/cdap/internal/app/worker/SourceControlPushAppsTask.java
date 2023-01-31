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

package io.cdap.cdap.internal.app.worker;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.sourcecontrol.InMemorySourceControlOperationRunner;
import io.cdap.cdap.sourcecontrol.operationrunner.PushApplicationsContext;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppsResponse;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * SourceControlPushAppsTask is a RunnableTask for performing pulling an application from linked repository.
 */
public class SourceControlPushAppsTask implements RunnableTask {
  private static final Gson GSON = new Gson();
  private final CConfiguration cConf;

  @Inject
  SourceControlPushAppsTask(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    PushApplicationsContext pushAppsContext = GSON.fromJson(context.getParam(), PushApplicationsContext.class);
    InMemorySourceControlOperationRunner operationRunner =
      new InMemorySourceControlOperationRunner(cConf, pushAppsContext.getRepositoryConfig());

    PushAppsResponse pushAppsResponse;
    try {
      pushAppsResponse = operationRunner.push(pushAppsContext.getAppsToPush(),
                                              pushAppsContext.getCommitDetails()).get(120, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      // We don't need the ExecutionException being reported back to the RemoteTaskExecutor, hence only
      // propagating the actual cause.
      Throwables.propagateIfPossible(e.getCause(), Exception.class);
      throw Throwables.propagate(e.getCause());
    }

    context.writeResult(GSON.toJson(pushAppsResponse).getBytes(StandardCharsets.UTF_8));
  }
}
