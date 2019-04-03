/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service.executor;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.proto.DatasetTypeMeta;

/**
 * Information for creating dataset instance.
 */
final class InternalDatasetUpdateParams extends InternalDatasetCreationParams {

  private final DatasetSpecification existingSpec;

  InternalDatasetUpdateParams(DatasetTypeMeta typeMeta,
                              DatasetSpecification existingSpec, DatasetProperties instanceProps) {
    super(typeMeta, instanceProps);
    this.existingSpec = existingSpec;
  }

  DatasetSpecification getExistingSpec() {
    return existingSpec;
  }
}
