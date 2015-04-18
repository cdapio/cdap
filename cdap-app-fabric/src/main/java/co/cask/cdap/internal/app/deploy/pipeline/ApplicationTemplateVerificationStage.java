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

import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.adapter.AdapterService;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;

/**
 * This {@link co.cask.cdap.pipeline.Stage} is responsible for verifying an ApplicationSpecification
 * for an ApplicationTemplate. It mostly verifies the same thing an Application verifies, except it expects
 * just one worker or workflow, and it does not check that the template already exists, since we need to be
 * able to update a template.
 */
public class ApplicationTemplateVerificationStage extends ApplicationVerificationStage {

  public ApplicationTemplateVerificationStage(Store store, DatasetFramework dsFramework,
                                              AdapterService adapterService) {
    super(store, dsFramework, adapterService);
  }

  /**
   * Receives an input containing application specification and location
   * and verifies both.
   *
   * @param input An instance of {@link ApplicationDeployable}
   */
  @Override
  public void process(ApplicationDeployable input) throws Exception {
    Preconditions.checkNotNull(input);

    Id.Application appId = input.getId();
    ApplicationSpecification specification = input.getSpecification();
    // Doing some verification here because ApplicationTemplate extends AbstractApplication,
    // but only supports a subset of what applications support.
    // TODO: change API so that a lot of this stuff isn't possible to do through the template API.
    if (!specification.getSchedules().isEmpty()) {
      throw new IllegalArgumentException("Schedules are not supported in application templates");
    }
    if (!specification.getFlows().isEmpty()) {
      throw new IllegalArgumentException("Flows are not supported in application templates");
    }
    if (!specification.getServices().isEmpty()) {
      throw new IllegalArgumentException("Services are not supported in application templates");
    }

    int numWorkflows = specification.getWorkflows().size();
    int numWorkers = specification.getWorkers().size();
    // templates can contain one workflow or one worker but not both.
    if (!((numWorkers == 0 && numWorkflows == 1) || (numWorkers == 1 && numWorkflows == 0))) {
      throw new IllegalArgumentException("An application template must contain exactly one worker or workflow");
    }

    verifySpec(appId, specification);
    verifyPrograms(appId, specification);

    emit(input);
  }
}
