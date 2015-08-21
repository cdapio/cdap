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

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Simple implementation of {@link DatasetDefinitionRegistry} that keeps state in memory.
 */
public class InMemoryDatasetDefinitionRegistry implements DatasetDefinitionRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryDatasetDefinitionRegistry.class);

  private Map<String, DatasetDefinition> datasetTypes = Maps.newHashMap();

  @Override
  public <T extends DatasetDefinition> T get(String datasetType) {
    DatasetDefinition def = datasetTypes.get(datasetType);
    if (def == null) {
      String msg = "Requested dataset type does NOT exist: " + datasetType;
      LOG.debug(msg);
      throw new IllegalArgumentException(msg);
    }
    return (T) def;
  }

  @Override
  public void add(DatasetDefinition def) {
    String typeName = def.getName();
    if (datasetTypes.containsKey(typeName)) {
      throw new TypeConflictException("Cannot add dataset type: it already exists: " + typeName);
    }
    datasetTypes.put(typeName, def);
  }

  @Override
  public boolean hasType(String typeName) {
    return datasetTypes.containsKey(typeName);
  }
}
