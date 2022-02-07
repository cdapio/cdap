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
 * Control messages sent from tether server to the client.
 */
public class TetheringControlMessage {
  /**
   * Control messages type.
   */
  public enum Type {
    KEEPALIVE,
    RUN_PIPELINE,
    STOP_PIPELINE
  }

  private final Type type;
  private final byte[] payload;

  public TetheringControlMessage(Type type) {
    this(type, new byte[0]);
  }

  public TetheringControlMessage(Type type, byte[] payload) {
    this.type = type;
    this.payload = payload;
  }

  public Type getType() {
    return type;
  }

  public byte[] getPayload() {
    return payload;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TetheringControlMessage that = (TetheringControlMessage) o;
    return Objects.equals(type, that.type) &&
      Objects.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, payload);
  }
}
