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

import com.google.inject.Inject;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewRequestQueue;
import io.cdap.cdap.app.preview.PreviewRequestQueueState;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.proto.id.ApplicationId;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Thread-safe implementation of {@link PreviewRequestQueue} backed by {@link PreviewStore}.
 */
@ThreadSafe
public class DefaultPreviewRequestQueue implements PreviewRequestQueue {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultPreviewRequestQueue.class);
  private final PreviewStore previewStore;
  private final BlockingDeque<PreviewRequest> requestQueue;
  private final int capacity;
  private final long waitTimeOut;
  private final RetryStrategy retryStrategy;

  @Inject
  public DefaultPreviewRequestQueue(CConfiguration cConf, PreviewStore previewStore) {
    this.previewStore = previewStore;
    this.capacity = cConf.getInt(Constants.Preview.WAITING_QUEUE_CAPACITY, 50);
    this.waitTimeOut = cConf.getLong(Constants.Preview.WAITING_QUEUE_TIMEOUT_SECONDS, 60);
    this.retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.preview.store.update.");
    this.requestQueue = previewStore.getAllInWaitingState().stream()
      .filter(r -> isValid(r, waitTimeOut))
      .collect(Collectors.toCollection(() -> new LinkedBlockingDeque<>(capacity)));
  }

  @Override
  public Optional<PreviewRequest> poll(byte[] pollerInfo) {
    while (true) {
      PreviewRequest previewRequest = requestQueue.poll();
      if (previewRequest == null) {
        return Optional.empty();
      }

      if (!isValid(previewRequest, waitTimeOut)) {
        LOG.warn("Preview request wth application id {} is timed out. Ignoring it.",
                 previewRequest.getProgram().getParent());
        continue;
      }

      try {
        PreviewRequest request = Retries.callWithRetries((Retries.Callable<PreviewRequest, Exception>) () -> {
          try {
            previewStore.setPreviewRequestPollerInfo(previewRequest.getProgram().getParent(), pollerInfo);
            return previewRequest;
          } catch (ConflictException e) {
            LOG.debug("Preview application with id {} is not present in WAITING state. Ignoring the preview.",
                      previewRequest.getProgram().getParent());
            return null;
          }
        }, retryStrategy, Retries.ALWAYS_TRUE);
        if (request != null) {
          return Optional.of(request);
        }
      } catch (Exception e) {
        LOG.error("Failed to set the poller info for preview application {}. Ignoring it.",
                  previewRequest.getProgram().getParent());
      }
    }
  }

  @Override
  public void add(PreviewRequest previewRequest) {
    if (!requestQueue.offer(previewRequest)) {
      throw new IllegalStateException(String.format("Preview request waiting queue is full with %d requests.",
                                                    requestQueue.size()));
    }
    previewStore.add(previewRequest.getProgram().getParent(), previewRequest.getAppRequest());
  }

  @Override
  public PreviewRequestQueueState getState() {
    return new PreviewRequestQueueState(requestQueue.size());
  }

  @Override
  public int positionOf(ApplicationId applicationId) {
    int position = 0;
    for (PreviewRequest request : requestQueue) {
      if (request.getProgram().getParent().equals(applicationId)) {
        return position;
      }
      position++;
    }
    return -1;
  }

  /**
   * Check if the request is valid and not timed out yet.
   */
  private static boolean isValid(PreviewRequest request, long waitTimeOut) {
    ApplicationId applicationId = request.getProgram().getParent();
    long submitTimeInSeconds = RunIds.getTime(applicationId.getApplication(), TimeUnit.SECONDS);
    long timeInWaiting = (System.currentTimeMillis() / 1000) - submitTimeInSeconds;
    return timeInWaiting <= waitTimeOut;
  }
}
