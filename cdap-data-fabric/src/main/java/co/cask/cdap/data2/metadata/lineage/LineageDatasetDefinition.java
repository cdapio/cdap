/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.lineage;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.IncompatibleUpdateException;
import co.cask.cdap.api.dataset.lib.CompositeDatasetDefinition;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableProperties;

import java.io.IOException;
import java.util.Map;

/**
 * {@link co.cask.cdap.api.dataset.DatasetDefinition} for {@link LineageDataset}.
 */
public class LineageDatasetDefinition extends CompositeDatasetDefinition<LineageDataset> {

  public static final String ACCESS_REGISTRY_TABLE = "access_registry";

  public LineageDatasetDefinition(String name, DatasetDefinition<Table, ? extends DatasetAdmin> tableDefinition) {
    super(name, ACCESS_REGISTRY_TABLE, tableDefinition);
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return super.configure(instanceName, noConflictDetection(properties));
  }

  @Override
  public DatasetSpecification reconfigure(String instanceName,
                                          DatasetProperties newProperties,
                                          DatasetSpecification currentSpec) throws IncompatibleUpdateException {
    return super.reconfigure(instanceName, noConflictDetection(newProperties), currentSpec);
  }

  private DatasetProperties noConflictDetection(DatasetProperties properties) {
    // Use ConflictDetection.NONE as we only need a flag whether a program uses a dataset/stream.
    // Having conflict detection will lead to failures when programs try to register accesses at the same time.
    return TableProperties.builder()
      .setConflictDetection(ConflictDetection.NONE)
      .addAll(properties.getProperties())
      .build();
  }

  @Override
  public LineageDataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                   Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    Table table = getDataset(datasetContext, ACCESS_REGISTRY_TABLE, spec, arguments, classLoader);
    return new LineageDataset(spec.getName(), table);
  }
}
