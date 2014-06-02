/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.app.ApplicationSpecification;
import com.continuuity.common.lang.jar.JarClassLoader;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.ModuleConflictException;
import com.continuuity.internal.data.dataset.module.DatasetModule;
import com.continuuity.pipeline.AbstractStage;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This {@link com.continuuity.pipeline.Stage} is responsible for automatic
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
   * @param input An instance of {@link com.continuuity.internal.app.deploy.pipeline.ApplicationSpecLocation}
   */
  @Override
  public void process(ApplicationSpecLocation input) throws Exception {
    // deploy dataset modules
    ApplicationSpecification specification = input.getSpecification();
    for (Map.Entry<String, String> module : specification.getDatasetModules().entrySet()) {
      // note: using app class loader to load module class
      JarClassLoader classLoader = new JarClassLoader(input.getArchive());
      @SuppressWarnings("unchecked")
      Class<? extends DatasetModule> moduleClass =
        (Class<DatasetModule>) classLoader.loadClass(module.getValue());
      String moduleName = module.getKey();
      try {
        datasetFramework.register(moduleName, moduleClass);
      } catch (ModuleConflictException e) {
        LOG.info("Not deploying module " + moduleName + " as it already exists");
      }
    }

    // Emit the input to next stage.
    emit(input);
  }
}
