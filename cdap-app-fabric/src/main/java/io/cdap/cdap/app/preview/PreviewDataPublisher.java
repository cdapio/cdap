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

import io.cdap.cdap.proto.id.EntityId;

/**
 * This interface exposes functionality for publishing preview data.
 */
public interface PreviewDataPublisher {

  /**
   * Publishes the {@link PreviewMessage} corresponding to the given entity id.
   *
   * @param entityId id of the entity with which message is to be associated.
   * @param previewMessage preview message
   */
  void publish(EntityId entityId, PreviewMessage previewMessage);

  /**
   * Sets the identifying information for the publisher of the preview data.
   * This is typically called once to associate the publisher with a specific preview runner.
   * The provided information should be a serialized {@code PreviewRequestPollerInfo} object,
   * which contains the unique identity of the runner pod. This information is used by the
   * receiving service to authenticate the origin of the messages.
   *
   * @param publisherInfo a byte array containing the serialized identifying information
   *                      for the publisher (the preview runner).
   */
  void setPublisherInfo(byte[] publisherInfo);
}
