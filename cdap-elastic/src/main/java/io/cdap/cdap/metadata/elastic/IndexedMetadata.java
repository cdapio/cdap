/*
 * Copyright © 2026 Cask Data, Inc.
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
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * The metadata of an entity, together with the sequence number and primary term of its document
 * in the index, which a conditional write of the document requires.
 */
final class IndexedMetadata {

  /** The metadata of an entity that has no document in the index. */
  static final IndexedMetadata NONE = new IndexedMetadata(Metadata.EMPTY, null, null);

  private final Metadata metadata;
  @Nullable
  private final Long seqNo;
  @Nullable
  private final Long primaryTerm;

  private IndexedMetadata(Metadata metadata, @Nullable Long seqNo, @Nullable Long primaryTerm) {
    this.metadata = metadata;
    this.seqNo = seqNo;
    this.primaryTerm = primaryTerm;
  }

  /**
   * Returns the metadata of an entity that has a document in the index.
   */
  static IndexedMetadata of(Metadata metadata, long seqNo, long primaryTerm) {
    return new IndexedMetadata(Objects.requireNonNull(metadata), seqNo, primaryTerm);
  }

  Metadata getMetadata() {
    return metadata;
  }

  @Nullable
  Long getSeqNo() {
    return seqNo;
  }

  @Nullable
  Long getPrimaryTerm() {
    return primaryTerm;
  }

  boolean hasDocument() {
    return seqNo != null;
  }
}
