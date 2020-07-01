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
import io.cdap.cdap.proto.id.ApplicationId;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe implementation of {@link PreviewRequestQueue} backed by {@link PreviewStore}.
 */
@ThreadSafe
public class DefaultPreviewRequestQueue implements PreviewRequestQueue {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultPreviewRequestQueue.class);
  private final PreviewStore previewStore;
  private final ConcurrentLinkedDeque<PreviewRequest> requestQueue;
  private final int capacity;
  private final long waitTimeOut;
  private final AtomicInteger queueSize;

  @Inject
  DefaultPreviewRequestQueue(CConfiguration cConf, PreviewStore previewStore) {
    this.previewStore = previewStore;
    this.capacity = cConf.getInt(Constants.Preview.WAITING_QUEUE_CAPACITY, 50);
    this.waitTimeOut = cConf.getLong(Constants.Preview.WAITING_QUEUE_TIMEOUT_SECONDS, 60);
    this.requestQueue = new ConcurrentLinkedDeque<>();
    this.queueSize = new AtomicInteger();
    List<PreviewRequest> allInWaitingState = previewStore.getAllInWaitingState();
    for (PreviewRequest request : allInWaitingState) {
      if (isTimedOut(request)) {
        continue;
      }
      requestQueue.add(request);
      queueSize.incrementAndGet();
    }
  }

  @Override
  public Optional<PreviewRequest> poll(byte[] pollerInfo) {
    while (true) {
      PreviewRequest previewRequest = requestQueue.poll();
      if (previewRequest == null) {
        return Optional.empty();
      }

      if (isTimedOut(previewRequest)) {
        LOG.warn("Preview request wth application id {} is timed out. Ignoring it.",
                 previewRequest.getProgram().getParent());
        continue;
      }

      try {
        previewStore.setPreviewRequestPollerInfo(previewRequest.getProgram().getParent(), pollerInfo);
      } catch (ConflictException e) {
        LOG.debug("Preview application with id {} is not present in WAITING state. Ignoring the preview.",
                  previewRequest.getProgram().getParent());
        continue;
      } catch (Exception e) {
        LOG.warn("Error while setting the poller info for preview request with application id {}. Trying again",
                 previewRequest.getProgram().getParent(), e);
        requestQueue.addFirst(previewRequest);
        continue;
      }

      queueSize.decrementAndGet();
      return Optional.of(previewRequest);
    }
  }

  @Override
  public void add(PreviewRequest previewRequest) {
    if (queueSize.intValue() >= capacity) {
      throw new IllegalStateException(String.format("Preview request waiting queue is full with %d requests.",
                                                    queueSize.intValue()));
    }
    previewStore.add(previewRequest.getProgram().getParent(), previewRequest.getAppRequest());
    requestQueue.add(previewRequest);
    queueSize.incrementAndGet();
  }

  @Override
  public PreviewRequestQueueState getState() {
    return new PreviewRequestQueueState(queueSize.get());
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

  private boolean isTimedOut(PreviewRequest request) {
    ApplicationId applicationId = request.getProgram().getParent();
    long submitTimeInSeconds = RunIds.getTime(applicationId.getApplication(), TimeUnit.SECONDS);
    long timeInWaiting = (System.currentTimeMillis() / 1000) - submitTimeInSeconds;
    return timeInWaiting > waitTimeOut;
  }
}
