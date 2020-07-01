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

package io.cdap.cdap.app.preview;

import com.google.gson.JsonObject;
import io.cdap.cdap.proto.id.ApplicationId;

import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Interface designed for holding {@link PreviewRequest}s that are in WAITING state.
 */
public interface PreviewRequestQueue {
  /**
   * Poll the next available request in the queue.
   * @param pollerInfo information about the poller in JSON format
   * @return {@code PreviewRequest} if such request is available in the queue
   */
  Optional<PreviewRequest> poll(@Nullable JsonObject pollerInfo);

  /**
   * Add a preview request in the queue.
   * @param previewRequest the request to be added to the waiting queue
   * @throws IllegalStateException if this queue is full
   */
  void add(PreviewRequest previewRequest);

  /**
   * Get the state of the preview request queue.
   */
  PreviewRequestQueueState getState();

  /**
   * Find the position of request with specified application id in the queue.
   * @param applicationId application id
   * @return -1 if application id does not exist
   */
  int positionOf(ApplicationId applicationId);
}
