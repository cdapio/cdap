/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap;

import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.CompositeDatasetDefinition;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.api.worker.AbstractWorker;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;

/**
 * App Template used to test adapter specific dataset and stream creation
 */
public class DataTemplate extends ApplicationTemplate<DataTemplate.Config> {
  public static final String NAME = "dsTemplate";

  @Override
  public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    configurer.setName(NAME);
    configurer.addWorker(new DWorker());
  }

  public static class Config {
    private final String streamName;
    private final String datasetName;

    public Config(String streamName, String datasetName) {
      this.streamName = streamName;
      this.datasetName = datasetName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Config that = (Config) o;

      return Objects.equal(streamName, that.streamName) && Objects.equal(datasetName, that.datasetName);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(streamName, datasetName);
    }
  }

  @Override
  public void configureAdapter(String adapterName, Config config, AdapterConfigurer configurer) throws Exception {
    configurer.addDatasetModule("customDataset", CustomDatasetModule.class);
    if (config.streamName != null && !config.streamName.isEmpty()) {
      configurer.addStream(new Stream(config.streamName));
    }
    if (config.datasetName != null && !config.datasetName.isEmpty()) {
      configurer.createDataset(config.datasetName, CustomDataset.class, DatasetProperties.EMPTY);
    }
  }

  /**
   * Dummy worker, doesn't do anything.
   */
  public static class DWorker extends AbstractWorker {
    @Override
    public void run() {
      // no-op
    }
  }

  /**
   * A custom dataset.
   */
  public static class CustomDataset extends AbstractDataset {

    private final Table table;

    public CustomDataset(String instanceName, Table table) {
      super(instanceName, table);
      this.table = table;
    }
  }

  /**
   * Dataset module for the custom dataset.
   */
  public static class CustomDatasetModule implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      DatasetDefinition<Table, DatasetAdmin> tableDefinition = registry.get("table");
      registry.add(new CustomDatasetDefinition("customDataset", tableDefinition));
      registry.add(new CustomDatasetDefinition(CustomDataset.class.getName(), tableDefinition));
    }
  }

  /**
   * Dataset definition for the custom dataset.
   */
  public static class CustomDatasetDefinition extends CompositeDatasetDefinition<CustomDataset> {

    public CustomDatasetDefinition(String name, DatasetDefinition<? extends Table, ?> tableDefinition) {
      super(name, ImmutableMap.of("table", tableDefinition));
    }

    @Override
    public CustomDataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                    Map<String, String> arguments, ClassLoader classLoader) throws IOException {
      Table table = getDataset(datasetContext, "table", Table.class, spec, arguments, classLoader);
      return new CustomDataset(spec.getName(), table);
    }
  }
}
