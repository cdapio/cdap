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
import io.cdap.cdap.etl.proto.v2.ETLConfig;

import java.util.Objects;

/**
 * A pipeline draft.
 */
public class Draft extends DraftStoreRequest<ETLConfig> {

  private final String id;
  private final long createdTimeMillis;
  private final long updatedTimeMillis;
  private final int configHash;

  private Draft(ETLConfig config, String previousHash, String name, String description,
                int revision,
                ArtifactSummary artifact, String id, long createdTimeMillis, long updatedTimeMillis) {
    super(config, previousHash, name, description, revision, artifact);
    this.id = id;
    this.createdTimeMillis = createdTimeMillis;
    this.updatedTimeMillis = updatedTimeMillis;
    this.configHash = config == null ? 0 : config.hashCode();
  }

  // This should be the default constructor until previousHash and revision are needed
  public Draft(ETLConfig config, String name, String description, ArtifactSummary artifact,
               String id,
               long createdTimeMillis, long updatedTimeMillis) {
    this(config, "", name, description, 0, artifact, id, createdTimeMillis, updatedTimeMillis);
  }

  public int getConfigHash() {
    return configHash;
  }

  public long getCreatedTimeMillis() {
    return createdTimeMillis;
  }

  public long getUpdatedTimeMillis() {
    return updatedTimeMillis;
  }

  public String getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    Draft draft = (Draft) o;
    return createdTimeMillis == draft.createdTimeMillis &&
      updatedTimeMillis == draft.updatedTimeMillis &&
      configHash == draft.configHash &&
      Objects.equals(id, draft.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), id, createdTimeMillis, updatedTimeMillis);
  }
}
