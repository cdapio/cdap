/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.writer;

import co.cask.cdap.proto.id.EntityId;

/**
 * A metadata publisher that does nothing.
 *
 * Note: this class is used for testing only. It should really be in the test directory for this module.
 * However, explore tests depend on this class, and expore tests cannot include the data-fabric test jar,
 * because that disables explore and fails all explore test classes.
 *
 */
// TODO (CDAP-14675): Move this class to tests.
public class NoOpMetadataPublisher implements MetadataPublisher {
  @Override
  public void publish(EntityId publisher, MetadataOperation metadataOperation) {
    // nop-op
  }

  @Override
  public void publish(EntityId entityId, DatasetInstanceOperation datasetInstanceOperation) {
    // no-op
  }
}
