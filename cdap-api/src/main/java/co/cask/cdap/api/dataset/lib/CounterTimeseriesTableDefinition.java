/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.IncompatibleUpdateException;
import co.cask.cdap.api.dataset.table.Table;

import java.io.IOException;
import java.util.Map;

/**
 * {@link co.cask.cdap.api.dataset.DatasetDefinition} for {@link CounterTimeseriesTable}.
 */
@Beta
public class CounterTimeseriesTableDefinition extends CompositeDatasetDefinition<CounterTimeseriesTable> {

  public CounterTimeseriesTableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDef) {
    super(name, "ts", tableDef);
  }

  @Override
  public DatasetSpecification reconfigure(String instanceName,
                                          DatasetProperties newProperties,
                                          DatasetSpecification currentSpec) throws IncompatibleUpdateException {
    TimeseriesTableDefinition.validateNewIntervalSize(newProperties, currentSpec);
    return super.reconfigure(instanceName, newProperties, currentSpec);
  }

  @Override
  public CounterTimeseriesTable getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                           Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    return new CounterTimeseriesTable(spec, this.<Table>getDataset(datasetContext, "ts", spec, arguments, classLoader));
  }
}
