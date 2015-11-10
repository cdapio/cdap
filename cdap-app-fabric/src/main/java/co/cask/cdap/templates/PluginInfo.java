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

package co.cask.cdap.templates;

/**
 * Contains plugin information.
 */
@Deprecated
public final class PluginInfo implements Comparable<PluginInfo> {

  private final String fileName;
  private final String name;
  private final PluginVersion version;

  public PluginInfo(String fileName, String name, PluginVersion version) {
    if (fileName == null) {
      throw new IllegalArgumentException("Plugin fileName cannot be null");
    }
    if (name == null) {
      throw new IllegalArgumentException("Plugin name cannot be null");
    }
    if (version == null) {
      throw new IllegalArgumentException("Plugin version cannot be null");
    }

    this.fileName = fileName;
    this.name = name;
    this.version = version;
  }

  /**
   * Returns file name of the plugin.
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * Returns name of the plugin.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns version of the plugin.
   */
  public PluginVersion getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "PluginInfo{" +
      "fileName='" + fileName + '\'' +
      ", name='" + name + '\'' +
      ", version=" + version +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PluginInfo that = (PluginInfo) o;
    return compareTo(that) == 0;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + version.hashCode();
    return result;
  }

  @Override
  public int compareTo(PluginInfo other) {
    int cmp = name.compareTo(other.name);
    if (cmp != 0) {
      return cmp;
    }
    return version.compareTo(other.version);
  }
}
