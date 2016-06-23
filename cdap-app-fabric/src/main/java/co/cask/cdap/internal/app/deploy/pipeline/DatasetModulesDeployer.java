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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.ModuleConflictException;
import co.cask.cdap.data2.dataset2.SingleTypeModule;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.NamespaceId;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Deploys Dataset Modules.
 */
final class DatasetModulesDeployer {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetModulesDeployer.class);

  private final DatasetFramework datasetFramework;
  // An instance of InMemoryDatasetFramework is used to check if a dataset is a system dataset
  private final DatasetFramework systemDatasetFramework;
  private final boolean allowDatasetUncheckedUpgrade;

  DatasetModulesDeployer(DatasetFramework datasetFramework,
                         DatasetFramework inMemoryDatasetFramework, CConfiguration cConf) {
    this.datasetFramework = datasetFramework;
    this.systemDatasetFramework = inMemoryDatasetFramework;
    this.allowDatasetUncheckedUpgrade = cConf.getBoolean(Constants.Dataset.DATASET_UNCHECKED_UPGRADE);
  }

  /**
   * Deploy the given dataset modules.
   *
   * @param namespaceId namespace to deploy to
   * @param modules the dataset modules to deploy
   * @param jarLocation the location of the jar file containing the modules
   * @throws Exception if there was a problem deploying a module
   */
  void deployModules(NamespaceId namespaceId, Map<String, String> modules,
                     Location jarLocation, ClassLoader artifactClassLoader) throws Exception {
    for (Map.Entry<String, String> moduleEntry : modules.entrySet()) {
      // note: using app class loader to load module class
      @SuppressWarnings("unchecked")
      Class<Dataset> clazz = (Class<Dataset>) artifactClassLoader.loadClass(moduleEntry.getValue());
      String moduleName = moduleEntry.getKey();
      try {
        // note: we can deploy module or create module from Dataset class
        // note: it seems dangerous to instantiate dataset module here, but this will be fine when we move deploy into
        //       isolated user's environment (e.g. separate yarn container)
        DatasetModuleId moduleId = namespaceId.datasetModule(moduleName);
        if (DatasetModule.class.isAssignableFrom(clazz)) {
          LOG.info("Adding module: {}", clazz.getName());
          datasetFramework.addModule(moduleId.toId(), (DatasetModule) clazz.newInstance(), jarLocation);
        } else if (Dataset.class.isAssignableFrom(clazz)) {
          if (!systemDatasetFramework.hasSystemType(clazz.getName())) {
            // checking if type is in already or force upgrade is allowed
            DatasetTypeId typeId = namespaceId.datasetType(clazz.getName());
            if (!datasetFramework.hasType(typeId.toId()) || allowDatasetUncheckedUpgrade) {
              LOG.info("Adding module: {}", clazz.getName());
              datasetFramework.addModule(moduleId.toId(), new SingleTypeModule(clazz), jarLocation);
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
  }
}
