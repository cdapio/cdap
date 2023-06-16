/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.proto.sourcecontrol;

public class NamespaceConnectionConfig {
  private final String connectionId;
  private final String connectionType;
  private final String name;
  private final String description;
  private final NamespaceConnectionPluginConfig plugin;
  private final boolean isDefault;
  private final boolean preConfigured;


  public NamespaceConnectionConfig(
      String connectionId,
      String connectionType,
      String name,
      String description,
      NamespaceConnectionPluginConfig plugin,
      boolean isDefault,
      boolean preConfigured) {
    this.connectionId = connectionId;
    this.connectionType = connectionType;
    this.name = name;
    this.description = description;
    this.plugin = plugin;
    this.isDefault = isDefault;
    this.preConfigured = preConfigured;
  }

  public String getConnectionId() {
    return connectionId;
  }

  public String getConnectionType() { return connectionType; }

  public String getName() { return name; }

  public String getDescription() { return description; }

  public NamespaceConnectionPluginConfig getPlugin() { return plugin; }

  public boolean getIsDefault() { return isDefault; }

  public boolean getPreConfigured() { return preConfigured; }
}
