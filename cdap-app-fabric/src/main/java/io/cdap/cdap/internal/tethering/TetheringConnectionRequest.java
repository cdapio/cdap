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
import javax.annotation.Nullable;

/**
 * Tethering request sent from the {@link TetheringAgentService} to the tethering server.
 */
public class TetheringConnectionRequest {

  private final List<NamespaceAllocation> namespaceAllocations;
  private final long requestTime;
  private final String description;

  public TetheringConnectionRequest(List<NamespaceAllocation> namespaceAllocations,
      long requestTime,
      @Nullable String description) {
    this.namespaceAllocations = namespaceAllocations;
    this.requestTime = requestTime;
    this.description = description;
  }

  public List<NamespaceAllocation> getNamespaceAllocations() {
    return namespaceAllocations;
  }

  public long getRequestTime() {
    return requestTime;
  }

  @Nullable
  public String getDescription() {
    return description;
  }
}
