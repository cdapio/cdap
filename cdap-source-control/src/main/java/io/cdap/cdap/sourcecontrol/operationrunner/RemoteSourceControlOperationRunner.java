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

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Singleton;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RemoteExecutionException;
import io.cdap.cdap.api.service.worker.RemoteTaskException;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.internal.remote.RemoteTaskExecutor;
import io.cdap.cdap.sourcecontrol.ApplicationManager;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.SourceControlAppConfigNotFoundException;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import io.cdap.cdap.sourcecontrol.worker.ListAppsTask;
import io.cdap.cdap.sourcecontrol.worker.PullAppTask;
import io.cdap.cdap.sourcecontrol.worker.PushAppTask;
import io.cdap.common.http.HttpRequestConfig;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Remote implementation for {@link SourceControlOperationRunner}.
 * Runs all git operation inside task worker.
 */
@Singleton
public class RemoteSourceControlOperationRunner extends
    AbstractIdleService implements SourceControlOperationRunner {

  private static final Gson GSON = new GsonBuilder().create();

  private static final Logger LOG = LoggerFactory.getLogger(RemoteSourceControlOperationRunner.class);
  private final RemoteTaskExecutor remoteTaskExecutor;

  private final RemoteExceptionProvider<NotFoundException> notFoundExceptionProvider =
      new RemoteExceptionProvider<>(NotFoundException.class, NotFoundException::new);
  private final RemoteExceptionProvider<SourceControlAppConfigNotFoundException> gitAppNotFoundExceptionProvider =
    new RemoteExceptionProvider<>(SourceControlAppConfigNotFoundException.class,
        SourceControlAppConfigNotFoundException::new);

  private final RemoteExceptionProvider<AuthenticationConfigException> authConfigExceptionProvider =
    new RemoteExceptionProvider<>(AuthenticationConfigException.class, AuthenticationConfigException::new);

  private final RemoteExceptionProvider<NoChangesToPushException> noChangeToPushExceptionProvider =
    new RemoteExceptionProvider<>(NoChangesToPushException.class, NoChangesToPushException::new);

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
  public PushAppResponse push(PushAppOperationRequest pushRequest) throws NoChangesToPushException,
    AuthenticationConfigException {
    try {
      RunnableTaskRequest request = RunnableTaskRequest.getBuilder(PushAppTask.class.getName())
          .withParam(GSON.toJson(pushRequest))
          .withNamespace(pushRequest.getNamespaceId().getNamespace())
          .build();

      LOG.trace("Pushing application {} to linked repository", pushRequest.getApp());
      byte[] result = remoteTaskExecutor.runTask(request);
      return GSON.fromJson(new String(result, StandardCharsets.UTF_8), PushAppResponse.class);
    } catch (RemoteExecutionException e) {
      throw propagateRemoteException(e, noChangeToPushExceptionProvider, authConfigExceptionProvider);
    } catch (Exception ex) {
      throw new SourceControlException(ex.getMessage(), ex);
    }
  }

  @Override
  public List<PushAppResponse> multiPush(MultiPushAppOperationRequest pushRequest,
      ApplicationManager appManager)
      throws NoChangesToPushException, AuthenticationConfigException {
    throw new UnsupportedOperationException("multi push not supported for RemoteSourceControlOperationRunner");
  }

  @Override
  public PullAppResponse<?> pull(PullAppOperationRequest pullRequest) throws NotFoundException,
    AuthenticationConfigException {
    try {
      RunnableTaskRequest request = RunnableTaskRequest.getBuilder(PullAppTask.class.getName())
          .withParam(GSON.toJson(pullRequest))
          .withNamespace(pullRequest.getApp().getNamespace())
          .build();

      LOG.trace("Pulling application {} from linked repository", pullRequest.getApp());
      byte[] result = remoteTaskExecutor.runTask(request);
      return GSON.fromJson(new String(result, StandardCharsets.UTF_8), PullAppResponse.class);
    } catch (RemoteExecutionException e) {
      throw propagateRemoteException(e, gitAppNotFoundExceptionProvider, authConfigExceptionProvider);
    } catch (Exception ex) {
      throw new SourceControlException(ex.getMessage(), ex);
    }
  }

  @Override
  public void multiPull(MultiPullAppOperationRequest pullRequest, Consumer<PullAppResponse<?>> consumer)
      throws NotFoundException, AuthenticationConfigException {
    throw new UnsupportedOperationException("multi pull not supported for RemoteSourceControlOperationRunner");
  }

  @Override
  public RepositoryAppsResponse list(NamespaceRepository nameSpaceRepository)
    throws AuthenticationConfigException, NotFoundException {
    try {
      RunnableTaskRequest request = RunnableTaskRequest.getBuilder(ListAppsTask.class.getName())
          .withParam(GSON.toJson(nameSpaceRepository))
          .withNamespace(nameSpaceRepository.getNamespaceId().getNamespace())
          .build();
      LOG.trace("Listing applications for namespace {} in linked repository", nameSpaceRepository.getNamespaceId());
      byte[] result = remoteTaskExecutor.runTask(request);
      return GSON.fromJson(new String(result, StandardCharsets.UTF_8), RepositoryAppsResponse.class);
    } catch (RemoteExecutionException e) {
      throw propagateRemoteException(e, notFoundExceptionProvider, authConfigExceptionProvider);
    } catch (Exception ex) {
      throw new SourceControlException(ex.getMessage(), ex);
    }
  }

  /**
   * Helper method to propagate underlying exceptions correctly from {@link RemoteExecutionException}.
   *
   * @param remoteException remote exception raised by task
   * @param propagateType1 provider for exception that should be propagated as is
   * @param propagateType2 provider for exception that should be propagated as is
   * @param <X1> exception type of propagateType1
   * @param <X2> exception type of propagateType2
   * @throws X1 if underlying exception is propagateType1
   * @throws X2 if underlying exception is propagateType2
   * @throws SourceControlException otherwise
   */
  private static <X1 extends Exception, X2 extends Exception> X1 propagateRemoteException(
    RemoteExecutionException remoteException,
    RemoteExceptionProvider<X1> propagateType1,
    RemoteExceptionProvider<X2> propagateType2
  ) throws X1, X2, SourceControlException {
    // Getting the actual RemoteTaskException which has the root cause stackTrace and error message
    RemoteTaskException remoteTaskException = remoteException.getCause();
    String exceptionClass = remoteTaskException.getRemoteExceptionClassName();
    String exceptionMessage = remoteTaskException.getMessage();
    Throwable cause = remoteTaskException.getCause();

    propagateIfInstanceOf(propagateType1, exceptionClass, cause, exceptionMessage);
    propagateIfInstanceOf(propagateType2, exceptionClass, cause, exceptionMessage);

    throw new SourceControlException(exceptionMessage, cause);
  }

  /**
   * Creates and throws an exception to type propagateType if the exceptionClass matches.
   *
   * @param exceptionClass actual exception class
   * @param propagateType provider class of exception to be matched
   * @param cause the cause of exception
   * @param exceptionMessage message for the exception
   * @param <X> exception type of propagateType
   * @throws X if exceptionClass and propagateType matches
   */
  private static <X extends Exception> void propagateIfInstanceOf(
    RemoteExceptionProvider<X> propagateType, String exceptionClass, Throwable cause, String exceptionMessage
  ) throws X {
    if (propagateType.isOfClass(exceptionClass)) {
      throw propagateType.createException(exceptionMessage, cause);
    }
  }

  @Override
  protected void startUp() throws Exception {
    // no-op.
  }

  @Override
  protected void shutDown() throws Exception {
    // no-op.
  }
}
