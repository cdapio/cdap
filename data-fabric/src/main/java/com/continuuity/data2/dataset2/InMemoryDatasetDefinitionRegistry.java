/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data2.dataset2;

import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Simple implementation of {@link DatasetDefinitionRegistry} that keeps state in memory.
 */
public class InMemoryDatasetDefinitionRegistry implements DatasetDefinitionRegistry {
  private Map<String, DatasetDefinition> datasetTypes = Maps.newHashMap();

  @Override
  public <T extends DatasetDefinition> T get(String datasetType) {
    return (T) datasetTypes.get(datasetType);
  }

  @Override
  public void add(DatasetDefinition def) {
    datasetTypes.put(def.getName(), def);
  }
}
