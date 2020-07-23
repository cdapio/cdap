/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewRunner;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * A scheduled service that periodically poll for new preview request and execute it.
 */
public class PreviewRunnerService extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(PreviewRunnerService.class);
  private static final Cancellable DUMMY_CANCELLABLE = () -> { };

  private final PreviewRunner previewRunner;
  private final PreviewRequestFetcher requestFetcher;
  private final long pollDelayMillis;
  private final int maxRuns;
  private final RetryStrategy retryStrategy;
  private final CountDownLatch stopLatch;
  private final AtomicReference<Cancellable> cancelPreview;

  public PreviewRunnerService(CConfiguration cConf, PreviewRunner previewRunner,
                              PreviewRequestFetcher previewRequestFetcher) {
    this.previewRunner = previewRunner;
    this.requestFetcher = previewRequestFetcher;
    this.pollDelayMillis = cConf.getLong(Constants.Preview.REQUEST_POLL_DELAY_MILLIS);
    this.maxRuns = cConf.getInt(Constants.Preview.MAX_RUNS);
    this.retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.preview.");
    this.stopLatch = new CountDownLatch(1);
    this.cancelPreview = new AtomicReference<>();
  }

  @Override
  protected void triggerShutdown() {
    Cancellable cancellable = cancelPreview.getAndSet(DUMMY_CANCELLABLE);
    stopLatch.countDown();
    if (cancellable != null) {
      cancellable.cancel();
    }
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting preview runner service");
  }

  @Override
  protected void run() {
    boolean terminated = false;
    int runs = 0;
    while (!terminated && (maxRuns <= 0 || runs < maxRuns)) {
      try {
        PreviewRequest request = getPreviewRequest();
        if (request == null) {
          // If there is no preview request, sleep for a while and poll again.
          terminated = Uninterruptibles.awaitUninterruptibly(stopLatch, pollDelayMillis, TimeUnit.MILLISECONDS);
          continue;
        }

        runs++;
        Future<PreviewRequest> future = previewRunner.startPreview(request);

        // If the cancelPreview was not null, this means the triggerShutdown was called while the
        // startPreview was call. If that's the case, stop the preview.
        if (cancelPreview.compareAndSet(null, () -> stopPreview(request))) {
          waitForCompletion(request, future);
        } else {
          stopPreview(request);
          terminated = true;
        }
      } catch (Exception e) {
        // This is a system error not caused by the app, hence log an error
        LOG.error("Failed to execute preview", e);
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Preview runner service completed");
  }

  @Nullable
  private PreviewRequest getPreviewRequest() throws IOException {
    return Retries.callWithRetries(requestFetcher::fetch, retryStrategy).orElse(null);
  }

  private void stopPreview(PreviewRequest request) {
    try {
      previewRunner.stopPreview(request.getProgram());
    } catch (Exception e) {
      LOG.error("Failed to stop preview for {}", request.getProgram());
    }
  }

  private void waitForCompletion(PreviewRequest request, Future<?> future) {
    try {
      Uninterruptibles.getUninterruptibly(future);
    } catch (ExecutionException e) {
      // Just log a debug if preview failed since it is expected for an application having execution failure
      LOG.debug("Preview for {} failed", request.getProgram(), e.getCause());
    }
  }
}
