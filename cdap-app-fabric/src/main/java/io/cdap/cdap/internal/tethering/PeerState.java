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

/**
 * Information about tethered peers that's returned by REST APIs.
 */
public class PeerState extends PeerBase {
  private final TetheringConnectionStatus connectionStatus;

  public PeerState(String name, String endpoint, TetheringStatus tetheringStatus, PeerMetadata metadata,
                   TetheringConnectionStatus connectionStatus) {
    super(name, endpoint, tetheringStatus, metadata);
    this.connectionStatus = connectionStatus;
  }

  public boolean isActive() {
    return connectionStatus == TetheringConnectionStatus.ACTIVE;
  }

  @Override
  public boolean equals(Object other) {
    if (!super.equals(other)) {
      return false;
    }
    PeerState that = (PeerState) other;
    return Objects.equals(this.connectionStatus, that.connectionStatus);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), connectionStatus);
  }
}
