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

package io.cdap.cdap.internal.tether;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Information about tethered peers that's returned by REST APIs.
 */
public class PeerStatus {
  private final String name;
  private final String endpoint;
  private final TetherStatus tetherStatus;
  private final PeerMetadata metadata;
  private final TetherConnectionStatus connectionStatus;

  public PeerStatus(String name, String endpoint, TetherStatus tetherStatus, PeerMetadata metadata,
                    TetherConnectionStatus connectionStatus) {
    this.name = name;
    this.endpoint = endpoint;
    this.tetherStatus = tetherStatus;
    this.metadata = metadata;
    this.connectionStatus = connectionStatus;
  }

  public String getName() {
    return name;
  }

  @Nullable
  public String getEndpoint() {
    return endpoint;
  }

  public TetherStatus getTetherStatus() {
    return tetherStatus;
  }

  public PeerMetadata getPeerMetadata() {
    return metadata;
  }

  public TetherConnectionStatus getConnectionStatus() {
    return connectionStatus;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    PeerStatus that = (PeerStatus) other;
    return Objects.equals(this.name, that.name) &&
      Objects.equals(this.endpoint, that.endpoint) &&
      Objects.equals(this.tetherStatus, that.tetherStatus) &&
      Objects.equals(this.metadata, that.metadata) &&
      Objects.equals(this.connectionStatus, that.connectionStatus);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, endpoint, tetherStatus, metadata, connectionStatus);
  }
}
