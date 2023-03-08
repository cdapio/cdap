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

package io.cdap.cdap.sourcecontrol.operationrunner;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RemoteExecutionException;
import io.cdap.cdap.api.service.worker.RemoteTaskException;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.internal.remote.RemoteTaskExecutor;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.worker.PullAppTask;
import io.cdap.cdap.sourcecontrol.worker.PushAppTask;
import io.cdap.common.http.HttpRequestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import javax.inject.Inject;

/**
 * Remote implementation for {@link SourceControlOperationRunner}.
 * Runs all git operation inside task worker.
 */
public class RemoteSourceControlOperationRunner implements SourceControlOperationRunner {

  private static final Gson GSON = new GsonBuilder().create();

  private static final Logger LOG = LoggerFactory.getLogger(RemoteSourceControlOperationRunner.class);
  private final RemoteTaskExecutor remoteTaskExecutor;

  @Inject
  RemoteSourceControlOperationRunner(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
                                     RemoteClientFactory remoteClientFactory) {
    int readTimeout = cConf.getInt(Constants.TaskWorker.SOURCE_CONTROL_HTTP_CLIENT_READ_TIMEOUT_MS);
    int connectTimeout = cConf.getInt(Constants.TaskWorker.SOURCE_CONTROL_HTTP_CLIENT_CONNECTION_TIMEOUT_MS);
    HttpRequestConfig httpRequestConfig = new HttpRequestConfig(connectTimeout, readTimeout, false);
    this.remoteTaskExecutor = new RemoteTaskExecutor(cConf, metricsCollectionService, remoteClientFactory,
                                                     RemoteTaskExecutor.Type.TASK_WORKER, httpRequestConfig);
  }

  @Override
  public PushAppResponse push(PushAppOperationRequest pushAppOperationRequest) throws NoChangesToPushException,
    AuthenticationConfigException {
    try {
      RunnableTaskRequest request = RunnableTaskRequest.getBuilder(PushAppTask.class.getName())
        .withParam(GSON.toJson(pushAppOperationRequest)).build();
      
      LOG.trace("Pushing application {} to linked repository", pushAppOperationRequest.getApp());
      byte[] result = remoteTaskExecutor.runTask(request);
      return GSON.fromJson(new String(result, StandardCharsets.UTF_8), PushAppResponse.class);
    } catch (RemoteExecutionException e) {
      // Getting the actual RemoteTaskException
      // which has the root cause stackTrace and error message
      RemoteTaskException remoteTaskException = e.getCause();
      String exceptionClass = remoteTaskException.getRemoteExceptionClassName();
      String exceptionMessage = remoteTaskException.getMessage();
      Throwable cause = remoteTaskException.getCause();

      if (NoChangesToPushException.class.getName().equals(exceptionClass)) {
        throw new NoChangesToPushException(exceptionMessage, cause);
      }

      if (AuthenticationConfigException.class.getName().equals(exceptionClass)) {
        throw new AuthenticationConfigException(exceptionMessage, cause);
      }

      throw new SourceControlException(exceptionMessage, cause);
    } catch (Exception ex) {
      throw new SourceControlException(ex.getMessage(), ex);
    }
  }

  @Override
  public PullAppResponse<?> pull(PulAppOperationRequest pulAppOperationRequest) throws NotFoundException,
    AuthenticationConfigException {
    try {
      RunnableTaskRequest request = RunnableTaskRequest.getBuilder(PullAppTask.class.getName())
          .withParam(GSON.toJson(pulAppOperationRequest)).build();

      LOG.trace("Pulling application {} from linked repository", pulAppOperationRequest.getApp());
      byte[] result = remoteTaskExecutor.runTask(request);
      return GSON.fromJson(new String(result, StandardCharsets.UTF_8), PullAppResponse.class);
    } catch (RemoteExecutionException e) {
      // Getting the actual RemoteTaskException
      // which has the root cause stackTrace and error message
      RemoteTaskException remoteTaskException = e.getCause();
      String exceptionClass = remoteTaskException.getRemoteExceptionClassName();
      String exceptionMessage = remoteTaskException.getMessage();
      Throwable cause = remoteTaskException.getCause();

      if (AuthenticationConfigException.class.getName().equals(exceptionClass)) {
        throw new AuthenticationConfigException(exceptionMessage, cause);
      }

      if (NotFoundException.class.getName().equals(exceptionClass)) {
        throw new NotFoundException(exceptionMessage, cause);
      }

      throw new SourceControlException(exceptionMessage, cause);
    } catch (Exception ex) {
      throw new SourceControlException(ex.getMessage(), ex);
    }
  }

  @Override
  public RepositoryAppsResponse list(NamespaceId namespace, RepositoryConfig repoConfig)
    throws AuthenticationConfigException, NotFoundException {
    // TODO: CDAP-20357, list applications in task worker
    throw new UnsupportedOperationException("Not implemented");  }
}
