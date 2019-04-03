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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.IncompatibleUpdateException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PedanticDatasetDefinition extends CompositeDatasetDefinition<Dataset> {

  PedanticDatasetDefinition(String name) {
    super(name, new HashMap<String, DatasetDefinition>());
  }

  @Override
  public DatasetSpecification reconfigure(String instanceName,
                                          DatasetProperties newProperties,
                                          DatasetSpecification currentSpec) throws IncompatibleUpdateException {
    String currentValue = currentSpec.getProperty("immutable");
    String newValue = newProperties.getProperties().get("immutable");
    if (!Objects.equals(currentValue, newValue)) {
      throw new IncompatibleUpdateException(String.format("Cannot change property 'immutable' from %s to %s",
                                                          currentValue, newValue));
    }
    return super.reconfigure(instanceName, newProperties, currentSpec);
  }

  @Override
  public Dataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                            Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    throw new UnsupportedOperationException("This class is only used for testing reconfigure()");
  }
}
