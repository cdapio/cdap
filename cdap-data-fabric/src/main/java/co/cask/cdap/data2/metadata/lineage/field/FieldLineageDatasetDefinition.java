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

package co.cask.cdap.data2.metadata.lineage.field;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.CompositeDatasetDefinition;
import co.cask.cdap.api.dataset.table.Table;

import java.io.IOException;
import java.util.Map;

/**
 * {@link DatasetDefinition} for {@link FieldLineageDataset}.
 */
public class FieldLineageDatasetDefinition extends CompositeDatasetDefinition<FieldLineageDataset> {
  public FieldLineageDatasetDefinition(String name, DatasetDefinition<Table, ? extends DatasetAdmin> tableDefinition) {
    super(name, "fieldLineage", tableDefinition);
  }


  @Override
  public FieldLineageDataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                        Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    Table table = getDataset(datasetContext, "fieldLineage", spec, arguments, classLoader);
    return new FieldLineageDataset(spec.getName(), table);
  }
}
