/*
 * Copyright 2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata.dataset;

import io.cdap.cdap.api.metadata.MetadataEntity;

/**
 * Represents a single metadata entry that is the result of a search.
 */
public class MetadataResultEntry {
  private final MetadataEntry metadataEntry;
  private final String searchTerm;

  public MetadataResultEntry(MetadataEntry metadataEntry, String searchTerm) {
    this.metadataEntry = metadataEntry;
    this.searchTerm = searchTerm;
  }

  /**
   * @return {@link MetadataEntry}
   */
  public MetadataEntry getMetadataEntry() {
    return this.metadataEntry;
  }

  /**
   * @return {@link MetadataEntity} to which the {@link MetadataResultEntry} belongs
   */
  public MetadataEntity getMetadataEntity() {
    return this.metadataEntry.getMetadataEntity();
  }

  /**
   * @return the key for the metadata
   */
  public String getKey() {
    return this.metadataEntry.getKey();
  }

  /**
   * @return the value for the metadata
   */
  public String getValue() {
    return this.metadataEntry.getValue();
  }

  /**
   * @return the term used to search for the metadata
   */
  public String getSearchTerm() {
    return searchTerm;
  }
}
