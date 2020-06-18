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
import io.cdap.cdap.app.preview.PreviewRequestPollerInfo;
import io.cdap.cdap.app.preview.PreviewRequestQueue;
import io.cdap.cdap.app.preview.PreviewRequestQueueState;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * Thread-safe implementation of {@link PreviewRequestQueue} backed by {@link PreviewStore}.
 * {@code poll} method is {@code synchronized} to avoid multiple pollers getting the same preview
 * request back. Similarly {@code add} method is {@link synchronized} to make sure elements in the queue
 * do not exceeds the capacity.
 *
 */
public class DefaultPreviewRequestQueue implements PreviewRequestQueue {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultPreviewRequestQueue.class);
  private final PreviewStore previewStore;
  private final Queue<PreviewRequest> requestQueue;
  private final int capacity;
  private final long waitTimeOut;

  @Inject
  DefaultPreviewRequestQueue(CConfiguration cConf, PreviewStore previewStore) {
    this.previewStore = previewStore;
    this.capacity = cConf.getInt(Constants.Preview.WAITING_QUEUE_CAPACITY, 50);
    this.waitTimeOut = cConf.getLong(Constants.Preview.WAITING_QUEUE_TIMEOUT_SECONDS, 60);
    this.requestQueue = new ConcurrentLinkedQueue<>();
    List<PreviewRequest> allInWaitingState = previewStore.getAllInWaitingState();
    for (PreviewRequest request : allInWaitingState) {
      if (!isTimedOut(request)) {
        requestQueue.add(request);
      }
    }
  }

  @Override
  public synchronized Optional<PreviewRequest> poll(PreviewRequestPollerInfo previewRequestPollerInfo) {
    while (true) {
      PreviewRequest previewRequest = requestQueue.peek();
      if (previewRequest == null) {
        return Optional.empty();
      }

      if (isTimedOut(previewRequest)) {
        LOG.warn("Preview request wth application id {} is timed out. Ignoring it.",
                 previewRequest.getProgram().getParent());
        continue;
      }

      previewStore.setPreviewRequestPollerInfo(previewRequest.getProgram().getParent(), previewRequestPollerInfo);
      requestQueue.poll();
      return Optional.of(previewRequest);
    }
  }

  @Override
  public synchronized void add(PreviewRequest previewRequest) {
    int size = requestQueue.size();
    if (size >= capacity) {
      throw new IllegalStateException(String.format("Preview request waiting queue is full with %d requests.", size));
    }
    previewStore.addToWaitingState(previewRequest.getProgram().getParent(), previewRequest.getAppRequest());
    requestQueue.add(previewRequest);
  }

  @Override
  public PreviewRequestQueueState getState() {
    return new PreviewRequestQueueState(requestQueue.size());
  }

  private boolean isTimedOut(PreviewRequest request) {
    ApplicationId applicationId = request.getProgram().getParent();
    long submitTimeInSeconds = RunIds.getTime(applicationId.getApplication(), TimeUnit.SECONDS);
    long timeInWaiting = (System.currentTimeMillis() / 1000) - submitTimeInSeconds;
    return timeInWaiting > waitTimeOut;
  }
}
