/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.api.artifact;

import io.cdap.cdap.api.annotation.Beta;

/**
 * Describes an artifact which is upgraded for an application.
 */
@Beta
public class UpgradedArtifact {

  private final ArtifactId oldArtifact;
  private final ArtifactId newArtifact;

  public UpgradedArtifact(ArtifactId oldArtifact, ArtifactId newArtifact) {
    this.oldArtifact = oldArtifact;
    this.newArtifact = newArtifact;
  }

  public ArtifactId getNewArtifact() {
    return newArtifact;
  }

  public ArtifactId getOldArtifact() {
    return oldArtifact;
  }

  @Override
  public String toString() {
    return "Old Artifact: " + oldArtifact.toString()
        + "New Artifact: " + newArtifact.toString();
  }
}
