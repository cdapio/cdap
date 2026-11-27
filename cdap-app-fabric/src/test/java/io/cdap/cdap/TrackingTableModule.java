/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap;

import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.lib.CompositeDatasetDefinition;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import java.io.IOException;
import java.util.Map;

/**
 * A {@link DatasetModule} that defines the {@link TrackingTable} dataset type. Tests register it as an
 * extension module of the dataset service through {@code dataset.extensions.modules}.
 */
public class TrackingTableModule implements DatasetModule {

  /**
   * Returns the name of the dataset type defined by this module.
   */
  public static String getTypeName() {
    return TrackingTable.class.getName();
  }

  @Override
  public void register(DatasetDefinitionRegistry registry) {
    registry.add(new TrackingTableDefinition(getTypeName(), registry.get("keyValueTable")));
  }

  /**
   * The {@link DatasetDefinition} of the {@link TrackingTable}.
   */
  private static final class TrackingTableDefinition extends CompositeDatasetDefinition<TrackingTable> {

    private static final String EMBEDDED_TABLE = "kv";

    TrackingTableDefinition(String name, DatasetDefinition keyValueTableDefinition) {
      super(name, EMBEDDED_TABLE, keyValueTableDefinition);
    }

    @Override
    public TrackingTable getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                    Map<String, String> arguments, ClassLoader classLoader) throws IOException {
      KeyValueTable table = getDataset(datasetContext, EMBEDDED_TABLE, spec, arguments, classLoader);
      return new TrackingTable(spec, table);
    }
  }
}
