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

import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.data.dataset.DatasetCreationSpec;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.InstanceConflictException;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.Id;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This {@link co.cask.cdap.pipeline.Stage} is responsible for automatic
 * deploy of the {@link co.cask.cdap.api.dataset.module.DatasetModule}s specified by application.
 */
public class CreateDatasetInstancesStage extends AbstractStage<ApplicationDeployable> {
  private static final Logger LOG = LoggerFactory.getLogger(CreateDatasetInstancesStage.class);
  private final DatasetFramework datasetFramework;

  public CreateDatasetInstancesStage(DatasetFramework datasetFramework) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.datasetFramework = datasetFramework;
  }

  /**
   * Receives an input containing application specification and location
   * and verifies both.
   *
   * @param input An instance of {@link ApplicationDeployable}
   */
  @Override
  public void process(ApplicationDeployable input) throws Exception {
    // create dataset instances
    ApplicationSpecification specification = input.getSpecification();
    Id.Namespace namespaceId = input.getId().getNamespace();
    for (Map.Entry<String, DatasetCreationSpec> instanceEntry : specification.getDatasets().entrySet()) {
      String instanceName = instanceEntry.getKey();
      Id.DatasetInstance instanceId = Id.DatasetInstance.from(namespaceId, instanceName);
      DatasetCreationSpec instanceSpec = instanceEntry.getValue();
      try {
        if (!datasetFramework.hasInstance(instanceId)) {
          datasetFramework.addInstance(instanceSpec.getTypeName(), instanceId, instanceSpec.getProperties());
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
