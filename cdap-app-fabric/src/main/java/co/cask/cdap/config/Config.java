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

package co.cask.cdap.config;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Configuration Class that holds an Id and properties as a Map.
 */
public final class Config {
  private final String id;
  private final Map<String, String> properties;

  /**
   * Constructor for Config Class.
   * @param name name of the configuration
   * @param properties map of properties
   */
  public Config(String name, Map<String, String> properties) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(properties);
    this.id = name;
    this.properties = ImmutableMap.copyOf(properties);
  }

  public String getId() {
    return id;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Config)) {
      return false;
    }

    Config config = (Config) o;
    return Objects.equal(this.id, config.id) && Objects.equal(this.properties, config.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.id, this.properties);
  }
}
