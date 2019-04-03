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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.IncompatibleUpdateException;
import co.cask.cdap.api.dataset.Reconfigurable;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.lib.CompositeDatasetAdmin;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Test dataset module
 */
public class TestModule2 implements DatasetModule {

  public static final String NOT_RECONFIGURABLE = "noreconf";
  public static final String DESCRIPTION = "dataset test module 2 description";

  @Override
  public void register(DatasetDefinitionRegistry registry) {
    registry.get("datasetType1");
    registry.add(createDefinition("datasetType2"));
  }

  DatasetDefinition createDefinition(String name) {
    return new DummyDatasetDefinition(name);
  }

  private static class DummyDatasetDefinition
    extends AbstractDatasetDefinition<Dataset, DatasetAdmin>
    implements Reconfigurable {

    DummyDatasetDefinition(String name) {
      super(name);
    }

    @Override
    public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
      return DatasetSpecification
        .builder(instanceName, getName())
        .setDescription(DESCRIPTION)
        // This is to test that the dataset summary returned by list() contains the original properties.
        // Original properties are different from the spec.getProperties() if the configure() method
        // modifies the original properties. That is what we mimic here.
        .properties(ImmutableMap.<String, String>builder()
                      .putAll(properties.getProperties())
                      .put("extra", "value")
                      .build())
        .build();
    }

    @Override
    public DatasetSpecification reconfigure(String instanceName,
                                            DatasetProperties properties,
                                            DatasetSpecification currentSpec) throws IncompatibleUpdateException {
      if (!Objects.equals(currentSpec.getProperties().get(NOT_RECONFIGURABLE),
                          properties.getProperties().get(NOT_RECONFIGURABLE))) {
        throw new IncompatibleUpdateException(String.format("Can't change %s from %s to %s. ", NOT_RECONFIGURABLE,
                                                            currentSpec.getProperties().get(NOT_RECONFIGURABLE),
                                                            properties.getProperties().get(NOT_RECONFIGURABLE)));
      }
      return configure(instanceName, properties);
    }

    @Override
    public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                                 ClassLoader classLoader) {
      return new CompositeDatasetAdmin(Collections.<String, DatasetAdmin>emptyMap());
    }

    @Override
    public Dataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                              Map<String, String> arguments, ClassLoader classLoader) throws IOException {
      return null;
    }
  }
}
