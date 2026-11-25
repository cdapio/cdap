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

package io.cdap.cdap.internal.app.deploy.pipeline;

import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.pipeline.AbstractStage;
import java.util.Map;

/**
 * This {@link io.cdap.cdap.pipeline.Stage} verifies that every dataset module and dataset type
 * declared by the application is defined by the system. Custom dataset modules are not supported,
 * so an application that declares one is rejected.
 */
public class ValidateDatasetTypesStage extends AbstractStage<ApplicationDeployable> {

  // An instance of InMemoryDatasetFramework is used to check if a dataset type is a system type
  private final DatasetFramework systemDatasetFramework;

  public ValidateDatasetTypesStage(DatasetFramework inMemoryDatasetFramework) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.systemDatasetFramework = inMemoryDatasetFramework;
  }

  /**
   * Validates the dataset modules specified in the given application spec.
   *
   * @param input An instance of {@link ApplicationDeployable}
   * @throws InvalidArtifactException if the application declares a dataset module or dataset
   *     type that is not defined by the system
   */
  @Override
  public void process(ApplicationDeployable input) throws Exception {
    Map<String, String> datasetModules = input.getSpecification().getDatasetModules();

    if (hasNonSystemDatasetModules(datasetModules)) {
      throw new InvalidArtifactException("Custom dataset module is not supported. "
          + "One of the dataset module is a custom module: " + datasetModules);
    }

    // Emit the input to next stage.
    emit(input);
  }

  /**
   * Returns {@code true} if the given set of dataset modules contains a dataset type that is not
   * defined by the system.
   */
  private boolean hasNonSystemDatasetModules(Map<String, String> modules)
      throws DatasetManagementException {
    for (String type : modules.values()) {
      if (!systemDatasetFramework.hasSystemType(type)) {
        return true;
      }
    }
    return false;
  }
}
