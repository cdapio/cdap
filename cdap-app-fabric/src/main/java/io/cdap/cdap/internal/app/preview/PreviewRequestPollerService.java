/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.preview;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.api.service.worker.RemoteExecutionException;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewRequestQueue;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.net.HttpURLConnection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreviewRequestPollerService extends AbstractScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultPreviewManager.class);

  private static final Gson GSON = new Gson();
  private ScheduledExecutorService executor;
  private final CConfiguration previewCConf;
  private final PreviewRequestQueue previewRequestQueue;
  private final RemoteClient previewRunnerClient;
  private final RetryStrategy retryStrategy;
  private final PreviewStore previewStore;

  @Inject
  PreviewRequestPollerService(CConfiguration previewCConf, PreviewRequestQueue previewRequestQueue,
      RemoteClientFactory remoteClientFactory, PreviewStore previewStore) {
    this.previewCConf = previewCConf;
    this.previewRequestQueue = previewRequestQueue;
    this.previewStore = previewStore;
    this.previewRunnerClient = remoteClientFactory.createRemoteClient(
        Constants.Service.PREVIEW_RUNNER, new DefaultHttpRequestConfig(false),
        Constants.Gateway.INTERNAL_API_VERSION_3);
    this.retryStrategy = RetryStrategies.fromConfiguration(previewCConf,
        Constants.Service.PREVIEW_RUNNER + ".");
    LOG.info("sidhdirenge - PreviewRequestPollerService initialized.");
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("preview-poller"));
    return executor;
  }

  @Override
  protected Scheduler scheduler() {
    long pollDelayMillis = previewCConf.getLong(Constants.Preview.REQUEST_POLL_DELAY_MILLIS);
    LOG.info("sidhdirenge - Scheduler set for poll {}", pollDelayMillis);
    return Scheduler.newFixedRateSchedule(0, pollDelayMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  protected void runOneIteration() throws Exception {
    LOG.info("sidhdirenge - looking for preview request");
    PreviewRequest previewRequest = previewRequestQueue.poll().orElse(null);
    if (previewRequest == null) {
      return;
    }
    LOG.info("sidhdirenge - found a preview request");
    // This try block prevents the service from crashing when a single request fails permanently after retries.
    try {
      Retries.callWithRetries(() -> {
        HttpRequest.Builder requestBuilder = previewRunnerClient.requestBuilder(HttpMethod.POST,
            "/" + Constants.Service.PREVIEW_RUNNER + "/run").withBody(GSON.toJson(previewRequest));
        HttpRequest httpRequest = requestBuilder.build();
        HttpResponse httpResponse = previewRunnerClient.execute(httpRequest);

        if (httpResponse.getResponseCode() == HttpResponseStatus.TOO_MANY_REQUESTS.code()) {
          // Look for available runner pod.
          throw new RetryableException(
              String.format("Received response code %s for %s", httpResponse.getResponseCode(),
                  previewRequest));
        }

        if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
          // This is a definitive failure, not a transient network issue.
          // Throw a RuntimeException to break the retry loop immediately.
          BasicThrowable basicThrowable = GSON.fromJson(httpResponse.getResponseBodyAsString(),
              BasicThrowable.class);
          throw new RuntimeException(RemoteExecutionException.fromBasicThrowable(basicThrowable));
        }

        byte[] pollerInfo = httpResponse.getResponseBody();
        previewStore.setPreviewRequestPollerInfo(previewRequest.getProgram().getParent(),
            pollerInfo);
        return null;
      }, retryStrategy, throwable -> (throwable instanceof RetryableException));
    } catch (Exception e) {
      // A single request failed permanently after exhausting all retries.
      // Log the error and move on to the next iteration.
      if (e instanceof RetryableException) {
        //TODO(sidhdirenge) : Check if we need to add the request back to queue.
        long submitTimeMillis = RunIds.getTime(previewRequest.getProgram().getApplication(),
            TimeUnit.MILLISECONDS);
        PreviewStatus status = new PreviewStatus(
            PreviewStatus.Status.KILLED_BY_INSUFFICIENT_RESOURCES, submitTimeMillis,
            new BasicThrowable(new Exception(
                "Preview run failed possibly as no preview runners were available."
                    + "Please try running preview again.")),
            null, null);
        previewStore.setPreviewStatus(previewRequest.getProgram().getParent(), status);
      } else {
        LOG.error("Failed to process preview request {} after all retries.", previewRequest, e);
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    if (executor != null) {
      executor.shutdownNow();
    }
  }
}
