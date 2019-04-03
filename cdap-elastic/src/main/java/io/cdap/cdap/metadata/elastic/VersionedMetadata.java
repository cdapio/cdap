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

import io.cdap.cdap.spi.metadata.Metadata;

import javax.annotation.Nullable;

/**
 * A metadata and it version in the index. Used for optimistic concurrency control.
 */
public class VersionedMetadata {
  private final Metadata metadata;
  private final Long version;

  static final VersionedMetadata NONE = new VersionedMetadata(Metadata.EMPTY, null);

  static VersionedMetadata of(Metadata metadata, long version) {
    return new VersionedMetadata(metadata, version);
  }

  private VersionedMetadata(Metadata metadata, @Nullable Long version) {
    this.metadata = metadata;
    this.version = version;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public Long getVersion() {
    return version;
  }

  public boolean existing() {
    return version != null;
  }

}
