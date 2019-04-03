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

package co.cask.cdap.data2.dataset2.lib.external;

import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;

import java.io.IOException;
import java.util.Map;

/**
 * Dataset definition for {@link ExternalDataset}.
 */
public class ExternalDatasetDefinition implements DatasetDefinition<ExternalDataset, ExternalDatasetAdmin> {

  private final String name;

  public ExternalDatasetDefinition(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .build();
  }

  @Override
  public ExternalDatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                                       ClassLoader classLoader) throws IOException {
    return new ExternalDatasetAdmin();
  }

  @Override
  public ExternalDataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                    Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    return new ExternalDataset(arguments);
  }
}
