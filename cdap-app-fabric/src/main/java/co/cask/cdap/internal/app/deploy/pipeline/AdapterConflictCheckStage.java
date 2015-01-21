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

import co.cask.cdap.internal.app.runtime.adapter.AdapterService;
import co.cask.cdap.pipeline.AbstractStage;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

/**
 * This {@link co.cask.cdap.pipeline.Stage} is responsible for ensuring that the application is not conflicting with
 * an adapterType.
 */
public class AdapterConflictCheckStage extends AbstractStage<ApplicationDeployable> {
  private final AdapterService adapterService;

  public AdapterConflictCheckStage(AdapterService adapterService) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.adapterService = adapterService;
  }

  /**
   * Receives an input containing application specification and location
   * and that the application specification doesn't have a name conflicting with an adapterType.
   *
   * @param input An instance of {@link ApplicationDeployable}
   */
  @Override
  public void process(ApplicationDeployable input) throws Exception {
    Preconditions.checkNotNull(input);
    // Don't allow user application with the same name as an application deployed by a system adapter.
    // System adapter do not get deployed to system namespace because they deal directly with data in user namespace
    String appId = input.getSpecification().getName();
    if (adapterService.getAdapterTypeInfo(appId) != null) {
      throw new RuntimeException(String.format("Cannot deploy app: %s - An adapter type exists with with the name.",
                                               appId));
    }
    // Emit the input to next stage.
    emit(input);
  }
}
