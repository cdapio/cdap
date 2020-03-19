/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.app.program;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.ProgramSpecification;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.ProgramId;

/**
 * Provides information about a program.
 */
public class ProgramDescriptor {

  private final ProgramId programId;
  private final ApplicationSpecification appSpec;
  private final ArtifactId artifactId;

  public ProgramDescriptor(ProgramId programId, ApplicationSpecification appSpec) {
    this(programId, appSpec, Artifacts.apiToProtoArtifactId(programId.getNamespaceId(), appSpec.getArtifactId()));
  }

  @VisibleForTesting
  public ProgramDescriptor(ProgramId programId, ApplicationSpecification appSpec, ArtifactId artifactId) {
    this.programId = programId;
    this.appSpec = appSpec;
    this.artifactId = artifactId;
  }

  /**
   * Returns the artifact id that contains the program is created from.
   */
  public ArtifactId getArtifactId() {
    return artifactId;
  }

  /**
   * Returns the program id.
   */
  public ProgramId getProgramId() {
    return programId;
  }

  /**
   * Returns the specification of this program.
   *
   * @param <T> The actual type of the {@link ProgramSpecification} returned.
   */
  @SuppressWarnings("unchecked")
  public <T extends ProgramSpecification> T getSpecification() {
    ProgramId id = getProgramId();
    switch (id.getType()) {
      case MAPREDUCE:
        return (T) getApplicationSpecification().getMapReduce().get(id.getProgram());
      case WORKFLOW:
        return (T) getApplicationSpecification().getWorkflows().get(id.getProgram());
      case SERVICE:
        return (T) getApplicationSpecification().getServices().get(id.getProgram());
      case SPARK:
        return (T) getApplicationSpecification().getSpark().get(id.getProgram());
      case WORKER:
        return (T) getApplicationSpecification().getWorkers().get(id.getProgram());
      default:
        // This shouldn't happen
        throw new IllegalStateException("Unsupported program type " + id.getType());
    }
  }

  /**
   * Returns the {@link ApplicationSpecification} that this program is defined in.
   */
  public ApplicationSpecification getApplicationSpecification() {
    return appSpec;
  }
}
