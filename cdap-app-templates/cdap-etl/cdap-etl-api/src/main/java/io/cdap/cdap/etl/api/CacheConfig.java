/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.api;

/**
 * Cache configuration.
  */
public final class CacheConfig {
  private final long expirySeconds;
  private final int maxSize;

  public CacheConfig(long expirySeconds, int maxSize) {
    this.expirySeconds = expirySeconds;
    this.maxSize = maxSize;
  }

  public CacheConfig() {
    this(0, 0);
  }

  /**
   * @return expiry after write in seconds
   */
  public long getExpirySeconds() {
    return expirySeconds;
  }

  /**
   * @return maximum number of elements in the cache
   */
  public int getMaxSize() {
    return maxSize;
  }
}
