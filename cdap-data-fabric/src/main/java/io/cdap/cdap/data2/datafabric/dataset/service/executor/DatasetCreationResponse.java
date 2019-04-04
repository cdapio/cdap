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

package io.cdap.cdap.data2.datafabric.dataset.service.executor;

import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.data2.metadata.system.SystemMetadata;

import javax.annotation.Nullable;

/**
 * {@link DatasetAdminService} response ofr creation and update of a dataset.
 */
public final class DatasetCreationResponse {

  private final DatasetSpecification spec;
  private final SystemMetadata metadata;

  DatasetCreationResponse(DatasetSpecification spec, @Nullable SystemMetadata metadata) {
    this.spec = spec;
    this.metadata = metadata;
  }

  public DatasetSpecification getSpec() {
    return spec;
  }

  @Nullable
  public SystemMetadata getMetadata() {
    return metadata;
  }
}
