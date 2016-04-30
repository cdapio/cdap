/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import java.util.Objects;

/**
 * Represents an entry in {@link org.apache.hadoop.conf.Configuration}
 * or {@link co.cask.cdap.common.conf.CConfiguration}.
 */
public final class ConfigEntry {
  private final String name;
  private final String value;
  private final String source;

  public ConfigEntry(String name, String value, String source) {
    this.name = name;
    this.value = value;
    this.source = source;
  }

  public String getName() {
    return name;
  }

  public String getValue() {
    return value;
  }

  public String getSource() {
    return source;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value, source);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final ConfigEntry other = (ConfigEntry) obj;
    return Objects.equals(this.name, other.name) &&
      Objects.equals(this.value, other.value) &&
      Objects.equals(this.source, other.source);
  }

  @Override
  public String toString() {
    return "ConfigEntry{" +
      "name='" + name + '\'' +
      ", value='" + value + '\'' +
      ", source='" + source + '\'' +
      '}';
  }
}
