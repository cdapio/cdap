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
 * Peer information that's persisted in the tethering store.
 */
public class PeerInfo extends PeerBase {
  private final long lastConnectionTime;

  public PeerInfo(String name, @Nullable String endpoint, TetheringStatus tetheringStatus, PeerMetadata metadata,
                  long requestTime) {
    this(name, endpoint, tetheringStatus, metadata, requestTime, 0);
  }

  public PeerInfo(String name, @Nullable String endpoint, TetheringStatus tetheringStatus,
                  PeerMetadata metadata, long requestTime, long lastConnectionTime) {
    super(name, endpoint, tetheringStatus, metadata, requestTime);
    this.lastConnectionTime = lastConnectionTime;
  }

  public long getLastConnectionTime() {
    return lastConnectionTime;
  }

  @Override
  public boolean equals(Object other) {
    if (!super.equals(other)) {
      return false;
    }
    PeerInfo that = (PeerInfo) other;
    return Objects.equals(this.lastConnectionTime, that.lastConnectionTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), lastConnectionTime);
  }
}
