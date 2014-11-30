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

package co.cask.cdap.namespace;

/**
 * Represents metadata for namespaces
 * TODO: Should this be renamed to NamespaceSpecification and moved to cdap-api?
 */
public class NamespaceMetadata {
  private String name;
  private String displayName;
  private String description;

  private NamespaceMetadata(final String name, final String displayName, final String description) {
    this.name = name;
    this.displayName = displayName;
    this.description = description;
  }

  public String getName() {
    return name;
  }

  public String getDisplayName() {
    return displayName;
  }

  public String getDescription() {
    return description;
  }

  /**
   * Builder used to build {@link co.cask.cdap.namespace.NamespaceMetadata}
   */
  public static final class Builder {
    private String name;
    private String displayName;
    private String description;

    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    public Builder setDisplayName(final String displayName) {
      this.displayName = displayName;
      return this;
    }

    public Builder setDescription(final String description) {
      this.description = description;
      return this;
    }

    public NamespaceMetadata build() {
      return new NamespaceMetadata(name, displayName, description);
    }
  }
  @Override
  public String toString() {
    return "NamespaceMetadata{" +
      "name='" + name + '\'' +
      ", displayName='" + displayName + '\'' +
      ", description='" + description + '\'' +
      '}';
  }
}
