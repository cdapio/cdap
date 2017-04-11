/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import co.cask.cdap.app.program.ProgramDescriptor;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Represents information of an application and all programs inside it that is undergoing deployment.
 */
public class ApplicationWithPrograms extends ApplicationDeployable {

  private final List<ProgramDescriptor> programDescriptors;

  public ApplicationWithPrograms(ApplicationDeployable applicationDeployable,
                                 Iterable<? extends ProgramDescriptor> programDescriptors) {
    super(applicationDeployable.getArtifactId(), applicationDeployable.getArtifactLocation(),
          applicationDeployable.getApplicationId(), applicationDeployable.getSpecification(),
          applicationDeployable.getExistingAppSpec(), applicationDeployable.getApplicationDeployScope(),
          applicationDeployable.getOwnerPrincipal(), applicationDeployable.canUpdateSchedules());
    this.programDescriptors = ImmutableList.copyOf(programDescriptors);
  }

  /**
   * Returns a list of {@link ProgramDescriptor} for programs inside the application being deployed.
   */
  public Iterable<ProgramDescriptor> getPrograms() {
    return programDescriptors;
  }
}
