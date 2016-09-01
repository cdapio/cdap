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
package co.cask.cdap.proto.metadata;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.proto.id.NamespacedId;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Represent the Metadata search result record.
 */
@Beta
public class MetadataSearchResultRecord {
  private final NamespacedId namespacedId;
  private final Map<MetadataScope, Metadata> metadata;

  public MetadataSearchResultRecord(NamespacedId namespacedId) {
    this.namespacedId = namespacedId;
    this.metadata = Collections.emptyMap();
  }

  public MetadataSearchResultRecord(NamespacedId namespacedId, Map<MetadataScope, Metadata> metadata) {
    this.namespacedId = namespacedId;
    this.metadata = metadata;
  }

  public NamespacedId getEntityId() {
    return namespacedId;
  }

  public Map<MetadataScope, Metadata> getMetadata() {
    return metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MetadataSearchResultRecord)) {
      return false;
    }
    MetadataSearchResultRecord that = (MetadataSearchResultRecord) o;
    return Objects.equals(namespacedId, that.namespacedId) &&
      Objects.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespacedId, metadata);
  }

  @Override
  public String toString() {
    return "MetadataSearchResultRecord{" +
      "namespacedId=" + namespacedId +
      ", metadata=" + metadata +
      '}';
  }
}
