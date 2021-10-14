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
 * Peer information that's persisted in the tether store.
 */
public class PeerInfo {
  private final String name;
  private final String endpoint;
  private final TetherStatus tetherStatus;
  private final PeerMetadata metadata;
  private final long lastConnectionTime;

  public PeerInfo(String name, @Nullable String endpoint, TetherStatus tetherStatus, PeerMetadata metadata) {
    this(name, endpoint, tetherStatus, metadata, 0);
  }

  public PeerInfo(String name, @Nullable String endpoint, TetherStatus tetherStatus,
                  PeerMetadata metadata, long lastConnectionTime) {
    this.name = name;
    this.endpoint = endpoint;
    this.tetherStatus = tetherStatus;
    this.metadata = metadata;
    this.lastConnectionTime = lastConnectionTime;
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

  public PeerMetadata getMetadata() {
    return metadata;
  }

  public long getLastConnectionTime() {
    return lastConnectionTime;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    PeerInfo that = (PeerInfo) other;
    return Objects.equals(this.name, that.name) &&
      Objects.equals(this.endpoint, that.endpoint) &&
      Objects.equals(this.tetherStatus, that.tetherStatus) &&
      Objects.equals(this.metadata, that.metadata) &&
      Objects.equals(this.lastConnectionTime, that.lastConnectionTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, endpoint, tetherStatus, metadata, lastConnectionTime);
  }
}
