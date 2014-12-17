/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.data;

import javax.annotation.Nullable;

/**
 * Namespace of the dataset.
 */
public enum Namespace {
  USER("user"),
  SYSTEM("system");

  private final String name;
  private final String prefix;

  Namespace(String name) {
    this.name = name;
    this.prefix = name + ".";
  }

  String getName() {
    return name;
  }

  public String namespace(String datasetName) {
    return prefix + datasetName;
  }

  // returns null if given name doesn't belong to namespace
  @Nullable
  public String fromNamespaced(String namespacedDatasetName) {
    if (!contains(namespacedDatasetName)) {
      return null;
    }
    return namespacedDatasetName.substring(prefix.length());
  }

  public boolean contains(String namespacedDatasetName) {
    return namespacedDatasetName.startsWith(prefix);
  }
}
