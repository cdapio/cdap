/*
 * Copyright © 2020 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.datapipeline.draft;

import io.cdap.cdap.api.artifact.ArtifactSummary;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Request to store a draft.
 *
 * @param <T> Type of config that this DraftRequest contains
 */
public class DraftStoreRequest<T> {
  private final String previousHash;
  private final String name;
  private final String description;
  private final int revision;
  private final ArtifactSummary artifact;
  private final T config;

  public DraftStoreRequest(@Nullable T config, String previousHash, String name, String description, int revision,
                           @Nullable ArtifactSummary artifact) {
    this.config = config;
    this.previousHash = previousHash;
    this.name = name;
    this.description = description;
    this.revision = revision;
    this.artifact = artifact;
  }

  public String getDescription() {
    return description;
  }

  public String getName() {
    return name == null ? "" : name;
  }

  @Nullable
  public ArtifactSummary getArtifact() {
    return artifact;
  }

  @Nullable
  public T getConfig() {
    return config;
  }

  /**
   * Getter for the revision number. This will be used later on to enable draft version tracking.
   *
   * @return
   */
  public int getRevision() {
    return revision;
  }

  /**
   * Getter for previousHash. This field will be used later on to detect collision in drafts. It represents the hashcode
   * that was provided by the backend when the draft was fetched. That hashcode will be passed when the client makes a
   * save request and the backend can check against the current hash to see if the draft has been modified since the
   * client fetched the draft.
   *
   * @return
   */
  public String getPreviousHash() {
    return previousHash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DraftStoreRequest<T> that = (DraftStoreRequest<T>) o;
    return Objects.equals(config, that.config) &&
      Objects.equals(previousHash, that.previousHash) &&
      Objects.equals(name, that.name) &&
      Objects.equals(artifact, that.artifact) &&
      revision == that.revision;
  }

  @Override
  public int hashCode() {
    return Objects.hash(config, previousHash, revision, artifact, name, description);
  }
}

