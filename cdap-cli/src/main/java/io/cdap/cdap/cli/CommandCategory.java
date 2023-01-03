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

package io.cdap.cdap.cli;

/**
 * Various categories.
 */
public enum CommandCategory {
  GENERAL("General"),
  APPLICATION_LIFECYCLE("Application Lifecycle"),
  ARTIFACT("Artifact"),
  DATASET("Dataset"),
  EGRESS("Egress"),
  METADATA_AND_LINEAGE("Metadata and Lineage"),
  METRICS("Metrics"),
  NAMESPACE("Namespace"),
  SECURITY("Security");

  final String name;

  CommandCategory(String name) {
    this.name = name;
  }

  public String getName() {
    return name.toUpperCase();
  }

  public String getOriginalName() {
    return name;
  }

  public static CommandCategory valueOfNameIgnoreCase(String name) {
    for (CommandCategory commandCategory : CommandCategory.values()) {
      if (name.equalsIgnoreCase(commandCategory.name())) {
        return commandCategory;
      }
    }
    throw new IllegalArgumentException("Invalid command category: " + name);
  }
}
