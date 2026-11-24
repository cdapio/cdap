/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.metadata.elastic;

import io.cdap.cdap.spi.metadata.MetadataChange;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;

/**
 * A simple class to pass around a write to the index, along with the metadata change that it
 * effects.
 */
final class RequestAndChange {

  private final BulkOperation operation;
  private final MetadataChange change;

  RequestAndChange(BulkOperation operation, MetadataChange change) {
    this.operation = operation;
    this.change = change;
  }

  BulkOperation getOperation() {
    return operation;
  }

  MetadataChange getChange() {
    return change;
  }
}
