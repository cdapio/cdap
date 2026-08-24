/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.api.security.store;

import io.cdap.cdap.api.annotation.Beta;
import java.util.Collections;
import java.util.Set;

/**
 * Information associated with the Secure Store.
 * <p>
 * Note: This is the caller-facing API counterpart to {@code io.cdap.cdap.securestore.spi.SecretManagerInfo}
 * in the SPI layer.
 */
@Beta
public class SecureStoreInfo {
  /**
   * Represents the capabilities of the Secure Store.
   */
  public enum Capability {
    SECRET_LEASING
  }

  private final Set<Capability> capabilities;

  public SecureStoreInfo(Set<Capability> capabilities) {
    this.capabilities = capabilities != null ? Collections.unmodifiableSet(capabilities) : Collections.emptySet();
  }

  public Set<Capability> getCapabilities() {
    return capabilities;
  }
}
