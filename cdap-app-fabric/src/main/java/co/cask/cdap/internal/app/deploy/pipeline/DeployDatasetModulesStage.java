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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.ModuleConflictException;
import co.cask.cdap.data2.dataset2.SingleTypeModule;
import co.cask.cdap.pipeline.AbstractStage;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This {@link co.cask.cdap.pipeline.Stage} is responsible for automatic
 * deploy of the {@link DatasetModule}s specified by application.
 */
public class DeployDatasetModulesStage extends AbstractStage<ApplicationDeployable> {
  private static final Logger LOG = LoggerFactory.getLogger(DeployDatasetModulesStage.class);
  private final DatasetFramework datasetFramework;
  // An instance of InMemoryDatasetFramework is used to check if a dataset is a system dataset
  private final DatasetFramework systemDatasetFramework;
  private final boolean allowDatasetUncheckedUpgrade;

  public DeployDatasetModulesStage(CConfiguration configuration,
                                   DatasetFramework datasetFramework, DatasetFramework inMemoryDatasetFramework) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.datasetFramework = datasetFramework;
    this.systemDatasetFramework = inMemoryDatasetFramework;
    this.allowDatasetUncheckedUpgrade = configuration.getBoolean(Constants.Dataset.DATASET_UNCHECKED_UPGRADE);
  }

  /**
   * Receives an input containing application specification and location
   * and verifies both.
   *
   * @param input An instance of {@link ApplicationDeployable}
   */
  @Override
  public void process(ApplicationDeployable input) throws Exception {
    // deploy dataset modules
    ApplicationSpecification specification = input.getSpecification();

    ClassLoader classLoader = input.getClassLoader();
    for (Map.Entry<String, String> moduleEntry : specification.getDatasetModules().entrySet()) {
      // note: using app class loader to load module class
      @SuppressWarnings("unchecked")
      Class<?> clazz = classLoader.loadClass(moduleEntry.getValue());
      String moduleName = moduleEntry.getKey();
      try {
        // note: we can deploy module or create module from Dataset class
        // note: it seems dangerous to instantiate dataset module here, but this will be fine when we move deploy into
        //       isolated user's environment (e.g. separate yarn container)
        if (DatasetModule.class.isAssignableFrom(clazz)) {
          datasetFramework.addModule(moduleName, (DatasetModule) clazz.newInstance());
        } else if (Dataset.class.isAssignableFrom(clazz)) {
          boolean isSystemDataset = systemDatasetFramework.hasType(clazz.getName());
          if (!isSystemDataset) {
            // checking if type is in already or force upgrade is allowed
            if (!datasetFramework.hasType(clazz.getName()) || allowDatasetUncheckedUpgrade) {
              LOG.info("Adding module: {}", clazz.getName());
              datasetFramework.addModule(moduleName, new SingleTypeModule((Class<Dataset>) clazz));
            }
          }
        } else {
          String msg = String.format(
            "Cannot use class %s to add dataset module: it must be of type DatasetModule or Dataset",
            clazz.getName());
          throw new IllegalArgumentException(msg);
        }
      } catch (ModuleConflictException e) {
        LOG.info("Not deploying module " + moduleName + " as it already exists");
      }
    }

    // Emit the input to next stage.
    emit(input);
  }
}
