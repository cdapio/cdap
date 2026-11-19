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
import javax.annotation.Nullable;

/**
 * The metadata of an entity as it was read from the index, with the sequence number and primary
 * term of its document. The index only accepts a write that names these two values if the
 * document did not change since it was read, which gives optimistic concurrency control.
 */
final class IndexedMetadata {

  /** The metadata of an entity that has no document in the index. */
  static final IndexedMetadata NONE = new IndexedMetadata(Metadata.EMPTY, null, null);

  private final Metadata metadata;
  private final Long seqNo;
  private final Long primaryTerm;

  static IndexedMetadata of(Metadata metadata, long seqNo, long primaryTerm) {
    return new IndexedMetadata(metadata, seqNo, primaryTerm);
  }

  private IndexedMetadata(Metadata metadata, @Nullable Long seqNo, @Nullable Long primaryTerm) {
    this.metadata = metadata;
    this.seqNo = seqNo;
    this.primaryTerm = primaryTerm;
  }

  Metadata getMetadata() {
    return metadata;
  }

  /** Returns the sequence number of the document, or null if the entity has no document. */
  @Nullable
  Long getSeqNo() {
    return seqNo;
  }

  /** Returns the primary term of the document, or null if the entity has no document. */
  @Nullable
  Long getPrimaryTerm() {
    return primaryTerm;
  }

  /** Returns whether the entity has a document in the index. */
  boolean existing() {
    return seqNo != null;
  }
}
