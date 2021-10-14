/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Metadata about a tethered peer.
 */
public class PeerMetadata {
  private final List<NamespaceAllocation> namespaceAllocations;
  // metadata associated with the peer. ex: project, region when peer is located
  private final Map<String, String> metadata;

  public PeerMetadata(List<NamespaceAllocation> namespaceAllocations, Map<String, String> metadata) {
    this.namespaceAllocations = namespaceAllocations;
    this.metadata = metadata;
  }

  public List<NamespaceAllocation> getNamespaceAllocations() {
    return namespaceAllocations;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    PeerMetadata that = (PeerMetadata) other;
    return Objects.equals(this.namespaceAllocations, that.namespaceAllocations) &&
      Objects.equals(this.metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceAllocations, metadata);
  }
}
