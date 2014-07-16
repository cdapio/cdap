/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.app.ApplicationSpecification;
import com.continuuity.data.dataset.DatasetCreationSpec;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.InstanceConflictException;
import com.continuuity.pipeline.AbstractStage;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This {@link com.continuuity.pipeline.Stage} is responsible for automatic
 * deploy of the {@link com.continuuity.api.dataset.module.DatasetModule}s specified by application.
 */
public class CreateDatasetInstancesStage extends AbstractStage<ApplicationSpecLocation> {
  private static final Logger LOG = LoggerFactory.getLogger(CreateDatasetInstancesStage.class);
  private final DatasetFramework datasetFramework;

  public CreateDatasetInstancesStage(DatasetFramework datasetFramework) {
    super(TypeToken.of(ApplicationSpecLocation.class));
    this.datasetFramework = datasetFramework;
  }

  /**
   * Receives an input containing application specification and location
   * and verifies both.
   *
   * @param input An instance of {@link ApplicationSpecLocation}
   */
  @Override
  public void process(ApplicationSpecLocation input) throws Exception {
    // create dataset instances
    ApplicationSpecification specification = input.getSpecification();
    for (Map.Entry<String, DatasetCreationSpec> instanceEntry : specification.getDatasets().entrySet()) {
      String instanceName = instanceEntry.getKey();
      DatasetCreationSpec instanceSpec = instanceEntry.getValue();
      try {
        if (!datasetFramework.hasInstance(instanceName)) {
          datasetFramework.addInstance(instanceSpec.getTypeName(), instanceName, instanceSpec.getProperties());
        }
      } catch (InstanceConflictException e) {
        // NO-OP: Instance is simply already created, possibly by an older version of this app OR a different app
        // TODO: verify that the created instance is from this app
        LOG.warn("Couldn't create dataset instance '" + instanceName + "' of type '" + instanceSpec.getTypeName(), e);
      }
    }

    // Emit the input to next stage.
    emit(input);
  }
}
