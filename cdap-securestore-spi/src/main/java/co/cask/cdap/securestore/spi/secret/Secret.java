/*
 * Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.cdap.securestore.spi.secret;

import java.util.Arrays;
import java.util.Objects;

/**
 * Represents sensitive data to be stored securely.
 */
public class Secret {
  private final byte[] data;
  private final SecretMetadata metadata;

  /**
   * Constructs a secret with the given sensitive data and metadata.
   *
   * @param data sensitive data
   * @param metadata metadata for this secret
   */
  public Secret(byte[] data, SecretMetadata metadata) {
    this.data = Arrays.copyOf(data, data.length);
    this.metadata = metadata;
  }

  /**
   * @return sensitive data represented by this secret.
   */
  public byte[] getData() {
    return data;
  }

  /**
   * @return metadata associated with this secret
   */
  public SecretMetadata getMetadata() {
    return metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Secret secret = (Secret) o;
    return Arrays.equals(data, secret.data) &&
      Objects.equals(metadata, secret.metadata);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(metadata);
    result = 31 * result + Arrays.hashCode(data);
    return result;
  }
}
