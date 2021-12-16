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

/**
 * Tethering request that's sent to the client.
 */
public class TetheringCreationRequest {
  // Name of the peer
  private final String peer;
  // Server endpoint
  private final String endpoint;
  // CDAP namespaces
  private final List<NamespaceAllocation> namespaceAllocations;
  // Metadata associated with this tethering
  private final Map<String, String> metadata;

  public TetheringCreationRequest(String peer, String endpoint,
                                  List<NamespaceAllocation> namespaceAllocations, Map<String, String> metadata) {
    this.peer = peer;
    this.endpoint = endpoint;
    this.namespaceAllocations = namespaceAllocations;
    this.metadata = metadata;
  }

  public String getPeer() {
    return peer;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public List<NamespaceAllocation> getNamespaceAllocations() {
    return namespaceAllocations;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

}
