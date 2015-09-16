/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.publisher;

import co.cask.cdap.proto.metadata.MetadataChangeRecord;

/**
 * Interface for publishing metadata changes, so interested external systems can be notified of the change. The payload
 * published is represented by {@link MetadataChangeRecord}. Implementations of this interface can publish changes
 * to messaging (pub-sub) systems.
 */
public interface MetadataChangePublisher {

  /**
   * Publishes a {@link MetadataChangeRecord} to a messaging system.
   *
   * @param change the {@link MetadataChangeRecord} to publish
   */
  void publish(MetadataChangeRecord change);
}
