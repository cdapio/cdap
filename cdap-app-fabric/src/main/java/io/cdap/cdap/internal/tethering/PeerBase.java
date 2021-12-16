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

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Information about a tethered peer.
 */
public class PeerBase {
  private final String name;
  private final String endpoint;
  private final TetheringStatus tetheringStatus;
  private final PeerMetadata metadata;

  public PeerBase(String name, @Nullable String endpoint, TetheringStatus tetheringStatus, PeerMetadata metadata) {
    this.name = name;
    this.endpoint = endpoint;
    this.tetheringStatus = tetheringStatus;
    this.metadata = metadata;
  }

  public String getName() {
    return name;
  }

  @Nullable
  public String getEndpoint() {
    return endpoint;
  }

  public TetheringStatus getTetheringStatus() {
    return tetheringStatus;
  }

  public PeerMetadata getMetadata() {
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
    PeerBase that = (PeerBase) other;
    return Objects.equals(this.name, that.name) &&
      Objects.equals(this.endpoint, that.endpoint) &&
      Objects.equals(this.tetheringStatus, that.tetheringStatus) &&
      Objects.equals(this.metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, endpoint, tetheringStatus, metadata);
  }
}
