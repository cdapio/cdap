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

package io.cdap.cdap.securestore.spi;

import java.util.Objects;

/**
 * SPI model representing a distributed lease lock on a secret in the underlying storage plugin.
 *
 * <p>Note: This is the plugin-facing SPI counterpart to {@code io.cdap.cdap.api.security.store.lease.SecureStoreLease}
 * in the API layer, mirroring the {@link io.cdap.cdap.securestore.spi.secret.SecretMetadata} vs
 * {@code SecureStoreMetadata} architectural pattern in CDAP.
 */
public class SecretLease {
  private final boolean acquired;
  private final String lockTimestamp;
  private final String lockHolder;

  private SecretLease(boolean acquired, String lockTimestamp, String lockHolder) {
    this.acquired = acquired;
    this.lockTimestamp = lockTimestamp;
    this.lockHolder = lockHolder;
  }

  public static SecretLease acquired(String lockTimestamp, String lockHolder) {
    return new SecretLease(true, lockTimestamp, lockHolder);
  }

  public static SecretLease failed() {
    return new SecretLease(false, null, null);
  }

  public boolean isAcquired() {
    return acquired;
  }

  public String getLockTimestamp() {
    return lockTimestamp;
  }

  public String getLockHolder() {
    return lockHolder;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SecretLease that = (SecretLease) o;
    return acquired == that.acquired
        && Objects.equals(lockTimestamp, that.lockTimestamp)
        && Objects.equals(lockHolder, that.lockHolder);
  }

  @Override
  public int hashCode() {
    return Objects.hash(acquired, lockTimestamp, lockHolder);
  }

  @Override
  public String toString() {
    return "SecretLease{"
        + "acquired=" + acquired
        + ", lockTimestamp='" + lockTimestamp + '\''
        + ", lockHolder='" + lockHolder + '\''
        + '}';
  }
}
