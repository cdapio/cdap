/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.metadata.elastic;

import io.cdap.cdap.common.utils.ProjectInfo;

/**
 * Information about the version of this metadata storage provider. It includes the version of CDAP,
 * the version of the metadata implementation, and a checksum of the index mappings file.
 *
 * This information is stored as metadata in the index mappings, to be abe to decide whether the
 * index needs to be migrated after an upgrade. Strictly speaking, the {@link #METADATA_VERSION}
 * should be sufficient to make this determination, but we record a checksum of the index mappings
 * as an additional safeguard, as it will allow detecting a change that neglected to bump the
 * {@link #METADATA_VERSION}.
 */
public class VersionInfo {

  /**
   * The current version of the Elasticsearch metadata provider. This must be changed any time
   * the indexing schema, index settings, or index mappings are changed in a way that requires
   * an index migration (reindexing of all metadata). This is information is stored in the
   * index metadata, and at upgrade time, it is used to determine whether migration is needed.
   */
  public static final int METADATA_VERSION = 1;

  private final ProjectInfo.Version cdapVersion;
  private final int metadataVersion;
  private final long mappingsChecksum;

  public VersionInfo(ProjectInfo.Version cdapVersion, int metadataVersion, long mappingsChecksum) {
    this.cdapVersion = cdapVersion;
    this.metadataVersion = metadataVersion;
    this.mappingsChecksum = mappingsChecksum;
  }

  public ProjectInfo.Version getCdapVersion() {
    return cdapVersion;
  }

  public int getMetadataVersion() {
    return metadataVersion;
  }

  public long getMappingsChecksum() {
    return mappingsChecksum;
  }
}
