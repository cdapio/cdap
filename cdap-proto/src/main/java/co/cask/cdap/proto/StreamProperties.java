/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.proto;

import com.google.common.base.Objects;

import javax.annotation.Nullable;

/**
 * Represents the properties of a stream.
 */
public final class StreamProperties {

  private final String name;
  private final long ttl;

  public StreamProperties(String name, long ttl) {
    this.name = name;
    this.ttl = ttl;
  }

  /**
   * @return Name of the stream.
   */
  public String getName() {
    return name;
  }
  /**
   * @return The time to live in seconds for events in this stream.
   */
  public long getTTL() {
    return ttl;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("name", name)
      .add("ttl", ttl)
      .toString();
  }
}
