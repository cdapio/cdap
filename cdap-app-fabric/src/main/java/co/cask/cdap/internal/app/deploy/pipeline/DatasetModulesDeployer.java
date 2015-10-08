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
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.ModuleConflictException;
import co.cask.cdap.data2.dataset2.SingleTypeModule;
import co.cask.cdap.proto.Id;
import com.google.common.base.Throwables;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Deploys Dataset Modules.
 */
public class DatasetModulesDeployer {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetModulesDeployer.class);
  private final CConfiguration cConf;
  private final DatasetFramework datasetFramework;
  // An instance of InMemoryDatasetFramework is used to check if a dataset is a system dataset
  private final DatasetFramework systemDatasetFramework;
  private final Id.Namespace namespace;
  private final boolean allowDatasetUncheckedUpgrade;

  public DatasetModulesDeployer(DatasetFramework datasetFramework, DatasetFramework inMemoryDatasetFramework,
                                Id.Namespace namespace, CConfiguration cConf) {
    this.datasetFramework = datasetFramework;
    this.systemDatasetFramework = inMemoryDatasetFramework;
    this.namespace = namespace;
    this.cConf = cConf;
    this.allowDatasetUncheckedUpgrade = cConf.getBoolean(Constants.Dataset.DATASET_UNCHECKED_UPGRADE);
  }

  /**
   * Deploy the given dataset modules.
   *
   * @param modules the dataset modules to deploy
   * @param jarLocation the location of the jar file containing the modules
   * @throws Exception if there was a problem deploying a module
   */
  public void deployModules(Map<String, String> modules, Location jarLocation) throws Exception {
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    File unpackDir = DirUtils.createTempDir(tmpDir);

    try {
      ClassLoader classLoader = getClassLoader(jarLocation, unpackDir);

      for (Map.Entry<String, String> moduleEntry : modules.entrySet()) {
        // note: using app class loader to load module class
        @SuppressWarnings("unchecked")
        Class<?> clazz = classLoader.loadClass(moduleEntry.getValue());
        String moduleName = moduleEntry.getKey();
        try {
          // note: we can deploy module or create module from Dataset class
          // note: it seems dangerous to instantiate dataset module here, but this will be fine when we move deploy into
          //       isolated user's environment (e.g. separate yarn container)
          Id.DatasetModule moduleId = Id.DatasetModule.from(namespace, moduleName);
          if (DatasetModule.class.isAssignableFrom(clazz)) {
            datasetFramework.addModule(moduleId, (DatasetModule) clazz.newInstance());
          } else if (Dataset.class.isAssignableFrom(clazz)) {
            if (!systemDatasetFramework.hasSystemType(clazz.getName())) {
              // checking if type is in already or force upgrade is allowed
              Id.DatasetType typeId = Id.DatasetType.from(namespace, clazz.getName());
              if (!datasetFramework.hasType(typeId) || allowDatasetUncheckedUpgrade) {
                LOG.info("Adding module: {}", clazz.getName());
                datasetFramework.addModule(moduleId, new SingleTypeModule((Class<Dataset>) clazz));
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
    } finally {
      DirUtils.deleteDirectoryContents(unpackDir);
    }
  }

  private ClassLoader getClassLoader(Location jarLocation, File unpackDir) {
    try {
      BundleJarUtil.unpackProgramJar(jarLocation, unpackDir);

      // Create a ProgramClassLoader with the CDAP system ClassLoader as filter parent
      return ProgramClassLoader.create(cConf, unpackDir, ApplicationDeployable.class.getClassLoader());
    } catch (Exception e) {
      try {
        DirUtils.deleteDirectoryContents(unpackDir);
      } catch (IOException ioe) {
        // OK to ignore. Just log a warn.
        LOG.warn("Failed to delete directory {}", unpackDir, ioe);
      }
      throw Throwables.propagate(e);
    }
  }
}
