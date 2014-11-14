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
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.ModuleConflictException;
import co.cask.cdap.data2.dataset2.SingleTypeModule;
import co.cask.cdap.pipeline.AbstractStage;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * This {@link co.cask.cdap.pipeline.Stage} is responsible for automatic
 * deploy of the {@link DatasetModule}s specified by application.
 */
public class DeployDatasetModulesStage extends AbstractStage<ApplicationSpecLocation> {
  private static final Logger LOG = LoggerFactory.getLogger(DeployDatasetModulesStage.class);
  private final DatasetFramework datasetFramework;

  public DeployDatasetModulesStage(DatasetFramework datasetFramework) {
    super(TypeToken.of(ApplicationSpecLocation.class));
    this.datasetFramework = datasetFramework;
  }

  /**
   * Receives an input containing application specification and location
   * and verifies both.
   *
   * @param input An instance of {@link co.cask.cdap.internal.app.deploy.pipeline.ApplicationSpecLocation}
   */
  @Override
  public void process(ApplicationSpecLocation input) throws Exception {
    // deploy dataset modules
    ApplicationSpecification specification = input.getSpecification();
    File unpackedLocation = Files.createTempDir();
    try {
      BundleJarUtil.unpackProgramJar(input.getArchive(), unpackedLocation);
      ClassLoader classLoader = ProgramClassLoader.create(unpackedLocation, getClass().getClassLoader());
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
            // checking if type is in already
            if (!datasetFramework.hasType(clazz.getName())) {
              datasetFramework.addModule(moduleName, new SingleTypeModule((Class<Dataset>) clazz));
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
      try {
        DirUtils.deleteDirectoryContents(unpackedLocation);
      } catch (IOException e) {
        LOG.warn("Failed to delete directory {}", unpackedLocation, e);
      }
    }
    // Emit the input to next stage.
    emit(input);
  }
}
