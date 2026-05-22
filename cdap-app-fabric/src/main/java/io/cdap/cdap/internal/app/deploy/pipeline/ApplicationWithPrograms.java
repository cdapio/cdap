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

package io.cdap.cdap.internal.app.deploy.pipeline;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.app.program.ProgramDescriptor;
import java.util.List;

/**
 * Represents information of an application and all programs inside it that is undergoing
 * deployment.
 */
public class ApplicationWithPrograms extends ApplicationDeployable {

  private final List<ProgramDescriptor> programDescriptors;
  private final boolean deploySkipped;

  public ApplicationWithPrograms(ApplicationDeployable applicationDeployable,
      Iterable<? extends ProgramDescriptor> programDescriptors, boolean deploySkipped) {
    super(applicationDeployable.getArtifactId(), applicationDeployable.getArtifactLocation(),
        applicationDeployable.getApplicationId(), applicationDeployable.getSpecification(),
        applicationDeployable.getExistingAppSpec(),
        applicationDeployable.getApplicationDeployScope(),
        applicationDeployable.getApplicationClass(), applicationDeployable.getOwnerPrincipal(),
        applicationDeployable.canUpdateSchedules(), applicationDeployable.getSystemTables(),
        applicationDeployable.getMetadata(), applicationDeployable.getChangeDetail(),
        applicationDeployable.getSourceControlMeta(), applicationDeployable.isUpgrade(),
        applicationDeployable.isSkipMarkingLatest());
    this.programDescriptors = ImmutableList.copyOf(programDescriptors);
    this.deploySkipped = deploySkipped;
  }

  public ApplicationWithPrograms(ApplicationDeployable applicationDeployable,
      Iterable<? extends ProgramDescriptor> programDescriptors) {
    this(applicationDeployable, programDescriptors, false);
  }

  /**
   * Returns true if the deployment was skipped because it was a duplicate request.
   */
  public boolean isDeploySkipped() {
    return deploySkipped;
  }

  /**
   * Returns a list of {@link ProgramDescriptor} for programs inside the application being
   * deployed.
   */
  public Iterable<ProgramDescriptor> getPrograms() {
    return programDescriptors;
  }
}
