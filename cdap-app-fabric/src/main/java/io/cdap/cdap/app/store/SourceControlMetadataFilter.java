/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.app.store;

/**
 * A filter for source control metadata.
 */
public class SourceControlMetadataFilter {
  private final String nameContains;
  private final Boolean isSynced;

  public String getNameContains() {
    return nameContains;
  }

  public Boolean getIsSynced() {
    return isSynced;
  }

  public SourceControlMetadataFilter(String nameContains, Boolean syncStatus) {
    this.nameContains = nameContains;
    this.isSynced = syncStatus;
  }

  public static SourceControlMetadataFilter emptyFilter() {
    return new SourceControlMetadataFilter(null, null);
  }
}
