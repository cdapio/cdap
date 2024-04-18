/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.proto;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents a record of source control metadata.
 * This class includes all fields corresponding to the {@code NamespaceSourceControlMetadataStore}
 * and {@code RepositorySourceControlMetadataStore} tables.
 */
public class SourceControlMetadataRecord {

  private final String namespace;
  private final String type;
  private final String name;
  private final String specificationHash;
  @Nullable
  private final String commitId;
  @Nullable
  private final Long lastModified;
  private final Boolean isSynced;

  /**
   * Creates a new {@link SourceControlMetadataRecord}.
   */
  public SourceControlMetadataRecord(String namespace, String type, String name,
      String specificationHash, @Nullable String commitId, @Nullable Long lastModified, Boolean isSynced) {
    this.namespace = namespace;
    this.type = type;
    this.name = name;
    this.specificationHash = specificationHash;
    this.commitId = commitId;
    this.lastModified = lastModified;
    this.isSynced = isSynced;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public String getSpecificationHash() {
    return specificationHash;
  }

  @Nullable
  public String getCommitId() {
    return commitId;
  }

  @Nullable
  public Long getLastModified() {
    return lastModified;
  }

  public Boolean getIsSynced() {
    return isSynced;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SourceControlMetadataRecord record = (SourceControlMetadataRecord) o;
    return Objects.equals(namespace, record.namespace) && Objects.equals(type,
        record.type) && Objects.equals(name, record.name) && Objects.equals(
        specificationHash, record.specificationHash) && Objects.equals(commitId,
        record.commitId) && Objects.equals(lastModified, record.lastModified)
        && Objects.equals(isSynced, record.isSynced);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, type, name, specificationHash, commitId, lastModified, isSynced);
  }
}
