/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.ModuleConflictException;
import co.cask.cdap.data2.dataset2.SingleTypeModule;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.authorization.AuthorizationUtil;
import com.google.common.collect.Iterables;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

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
   * @param authorizingUser the authorizing user who will be making the call
   * @throws Exception if there was a problem deploying a module
   */
  void deployModules(NamespaceId namespaceId, Map<String, String> modules,
                     Location jarLocation, ClassLoader artifactClassLoader, String authorizingUser) throws Exception {
    List<String> implicitModules = new ArrayList<>();
    for (Map.Entry<String, String> moduleEntry : modules.entrySet()) {
      String moduleName = moduleEntry.getKey();
      String typeName = moduleEntry.getValue();
      if (systemDatasetFramework.hasSystemType(typeName)) {
        LOG.info("Not adding dataset type '{}' because it is defined by the system.", typeName);
        continue;
      }
      // Filter out the implicit modules: They have to be deployed last.
      if (moduleName.startsWith(".implicit.")) {
        implicitModules.add(typeName);
        continue;
      }
      loadAndDeployModule(artifactClassLoader, typeName, jarLocation, moduleName, namespaceId, authorizingUser);
    }
    for (String typeName : implicitModules) {
      final DatasetTypeId typeId = namespaceId.datasetType(typeName);

      DatasetTypeMeta typeMeta = AuthorizationUtil.authorizeAs(authorizingUser, new Callable<DatasetTypeMeta>() {
        @Override
        public DatasetTypeMeta call() throws Exception {
          return datasetFramework.getTypeInfo(typeId);
        }
      });
      if (typeMeta != null) {
        String existingModule = Iterables.getLast(typeMeta.getModules()).getName();
        if (modules.containsKey(existingModule)) {
          // it was deployed already as part of one of the explicit deployModule() calls
          continue;
        }
      }
      loadAndDeployModule(artifactClassLoader, typeName, jarLocation, typeName, namespaceId, authorizingUser);
    }
  }

  private  void loadAndDeployModule(ClassLoader artifactClassLoader, String className, final Location jarLocation,
                                    String moduleName, NamespaceId namespaceId,
                                    String authorizingUser) throws Exception {

    // note: using app class loader to load module class
    @SuppressWarnings("unchecked")
    Class<Dataset> clazz = (Class<Dataset>) artifactClassLoader.loadClass(className);
    try {
      // note: we can deploy module or create module from Dataset class
      // note: it seems dangerous to instantiate dataset module here, but this will be fine when we move deploy into
      //       isolated user's environment (e.g. separate yarn container)
      final DatasetModuleId moduleId = namespaceId.datasetModule(moduleName);
      final DatasetModule module;
      if (DatasetModule.class.isAssignableFrom(clazz)) {
        module = (DatasetModule) clazz.newInstance();
      } else if (Dataset.class.isAssignableFrom(clazz)) {
        if (systemDatasetFramework.hasSystemType(clazz.getName())) {
          return;
        }
        final DatasetTypeId typeId = namespaceId.datasetType(clazz.getName());
        boolean hasType = AuthorizationUtil.authorizeAs(authorizingUser, new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            return datasetFramework.hasType(typeId);
          }
        });
        if (hasType && !allowDatasetUncheckedUpgrade) {
          return;
        }
        module = new SingleTypeModule(clazz);
      } else {
        throw new IllegalArgumentException(String.format(
          "Cannot use class %s to add dataset module: it must be of type DatasetModule or Dataset", clazz.getName()));
      }
      LOG.info("Adding module: {}", clazz.getName());
      AuthorizationUtil.authorizeAs(authorizingUser, new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          datasetFramework.addModule(moduleId, module, jarLocation);
          return null;
        }
      });

    } catch (ModuleConflictException e) {
      LOG.info("Conflict while deploying module {}: {}", moduleName, e.getMessage());
      throw e;
    }
  }
}
