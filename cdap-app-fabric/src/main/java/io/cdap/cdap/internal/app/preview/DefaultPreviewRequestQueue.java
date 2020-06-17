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
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Implementation of {@link PreviewRequestQueue} backed by {@link PreviewStore}.
 */
public class DefaultPreviewRequestQueue implements PreviewRequestQueue {
  private final PreviewStore previewStore;
  private final ConcurrentLinkedQueue<PreviewRequest> requestQueue;
  private int capacity;

  @Inject
  DefaultPreviewRequestQueue(CConfiguration cConf, PreviewStore previewStore) {
    this.previewStore = previewStore;
    this.capacity = cConf.getInt(Constants.Preview.WAITING_QUEUE_CAPACITY, 50);
    List<PreviewRequest> allInWaitingState = previewStore.getAllInWaitingState();
    this.requestQueue = new ConcurrentLinkedQueue<>();
    requestQueue.addAll(allInWaitingState);
  }

  @Override
  public Optional<PreviewRequest> poll() {
    PreviewRequest previewRequest = requestQueue.poll();
    if (previewRequest == null) {
      return Optional.empty();
    }

    previewStore.removeFromWaitingState(previewRequest.getProgram().getParent());
    return Optional.of(previewRequest);
  }

  @Override
  public void add(PreviewRequest previewRequest) {
    int size = requestQueue.size();
    if (size >= capacity) {
      throw new IllegalStateException(String.format("Preview request waiting queue is full with %d requests.", size));
    }
    previewStore.addToWaitingState(previewRequest.getProgram().getParent(), previewRequest.getAppRequest());
    requestQueue.add(previewRequest);
  }

  @Override
  public PreviewRequestQueueState getState() {
    List<PreviewRequest> allWaiting = previewStore.getAllInWaitingState();
    return new PreviewRequestQueueState(allWaiting.size());
  }
}
