/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.proto.audit.payload.metadata;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.metadata.Metadata;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.proto.audit.AuditPayload;

import java.util.Map;
import java.util.Objects;

/**
 * Represents changes to metadata of an entity.
 */
@Beta
public class MetadataPayload extends AuditPayload {
  private final Map<MetadataScope, Metadata> previous;
  private final Map<MetadataScope, Metadata> additions;
  private final Map<MetadataScope, Metadata> deletions;

  public MetadataPayload(Map<MetadataScope, Metadata> previous,
                         Map<MetadataScope, Metadata> additions,
                         Map<MetadataScope, Metadata> deletions) {
    this.previous = previous;
    this.additions = additions;
    this.deletions = deletions;
  }

  public Map<MetadataScope, Metadata> getPrevious() {
    return previous;
  }

  public Map<MetadataScope, Metadata> getAdditions() {
    return additions;
  }

  public Map<MetadataScope, Metadata> getDeletions() {
    return deletions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MetadataPayload)) {
      return false;
    }
    MetadataPayload that = (MetadataPayload) o;
    return Objects.equals(previous, that.previous) &&
      Objects.equals(additions, that.additions) &&
      Objects.equals(deletions, that.deletions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(previous, additions, deletions);
  }

  @Override
  public String toString() {
    return "MetadataPayload{" +
      "previous=" + previous +
      ", additions=" + additions +
      ", deletions=" + deletions +
      "} " + super.toString();
  }
}
