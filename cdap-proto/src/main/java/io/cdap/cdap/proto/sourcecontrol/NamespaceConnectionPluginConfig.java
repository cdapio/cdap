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

import java.util.Map;

public class NamespaceConnectionPluginConfig {
  private final String category;
  private final String name;
  private final String type;
  private final Map<String, String> properties;
  private final NamespaceConnectionArtifact artifact;

  public NamespaceConnectionPluginConfig(
      String category,
      String name,
      String type,
      Map<String, String> properties,
      NamespaceConnectionArtifact artifact
  ) {
    this.category = category;
    this.name = name;
    this.type = type;
    this.properties = properties;
    this.artifact = artifact;
  }

  public String getCategory() { return category; }

  public String getName() { return name; }

  public String getType() { return type; }

  public Map<String, String> getProperties() { return properties; }

  public NamespaceConnectionArtifact getArtifact() { return artifact; }
}
