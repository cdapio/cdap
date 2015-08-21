/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.proto.DatasetTypeMeta;

/**
 * Dataset instance metadata.
 */
public class DatasetInstanceMeta {
  private final DatasetSpecification spec;

  // todo: meta of modules inside will have list of all types in the module that is redundant here
  private final DatasetTypeMeta type;

  public DatasetInstanceMeta(DatasetSpecification spec, DatasetTypeMeta type) {
    this.spec = spec;
    this.type = type;
  }

  public DatasetSpecification getSpec() {
    return spec;
  }

  public DatasetTypeMeta getType() {
    return type;
  }
}
