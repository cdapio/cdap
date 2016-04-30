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

import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.Id;
import com.google.common.reflect.TypeToken;

/**
 * This {@link co.cask.cdap.pipeline.Stage} is responsible for automatic
 * deploy of the {@link DatasetModule}s specified by application.
 */
public class DeployDatasetModulesStage extends AbstractStage<ApplicationDeployable> {
  private final DatasetModulesDeployer datasetModulesDeployer;

  public DeployDatasetModulesStage(CConfiguration configuration, Id.Namespace namespace,
                                   DatasetFramework datasetFramework, DatasetFramework inMemoryDatasetFramework) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.datasetModulesDeployer = new DatasetModulesDeployer(datasetFramework, inMemoryDatasetFramework,
                                                             namespace, configuration);
  }

  /**
   * Deploys dataset modules specified in the given application spec.
   *
   * @param input An instance of {@link ApplicationDeployable}
   */
  @Override
  public void process(ApplicationDeployable input) throws Exception {

    datasetModulesDeployer.deployModules(input.getSpecification().getDatasetModules(), input.getLocation());

    // Emit the input to next stage.
    emit(input);
  }
}
