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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import javax.annotation.Nullable;

/**
 * A write of a single document to the index: create it, replace it, or delete it.
 */
final class WriteOperation {

  /**
   * The kind of write. The name of each constant in lower case is the name of the action in a
   * bulk request.
   */
  enum Type {
    /** Add a document that must not exist yet. */
    CREATE,
    /** Replace a document, on the condition that it did not change since it was read. */
    INDEX,
    /** Remove a document. */
    DELETE
  }

  private final Type type;
  private final String documentId;
  private final JsonElement source;
  private final Long ifSeqNo;
  private final Long ifPrimaryTerm;

  private WriteOperation(Type type, String documentId, @Nullable JsonElement source,
      @Nullable Long ifSeqNo, @Nullable Long ifPrimaryTerm) {
    this.type = type;
    this.documentId = documentId;
    this.source = source;
    this.ifSeqNo = ifSeqNo;
    this.ifPrimaryTerm = ifPrimaryTerm;
  }

  /**
   * Returns a write that stores a document. If the entity has no document yet, the write only
   * succeeds if there is still none. Otherwise it only succeeds if the document is unchanged.
   */
  static WriteOperation store(String documentId, JsonElement source, IndexedMetadata before) {
    return before.existing()
        ? new WriteOperation(Type.INDEX, documentId, source, before.getSeqNo(),
            before.getPrimaryTerm())
        : new WriteOperation(Type.CREATE, documentId, source, null, null);
  }

  /**
   * Returns a write that deletes a document. If the entity has a document, the write only
   * succeeds if that document is unchanged.
   */
  static WriteOperation delete(String documentId, IndexedMetadata before) {
    return new WriteOperation(Type.DELETE, documentId, null, before.getSeqNo(),
        before.getPrimaryTerm());
  }

  Type getType() {
    return type;
  }

  String getDocumentId() {
    return documentId;
  }

  /** Returns the document to store, or null for a delete. */
  @Nullable
  JsonElement getSource() {
    return source;
  }

  /** Returns the sequence number that the document must still have, or null for no condition. */
  @Nullable
  Long getIfSeqNo() {
    return ifSeqNo;
  }

  /** Returns the primary term that the document must still have, or null for no condition. */
  @Nullable
  Long getIfPrimaryTerm() {
    return ifPrimaryTerm;
  }

  /**
   * Returns the action line that describes this write in the body of a bulk request.
   */
  JsonObject toBulkAction(String indexName) {
    JsonObject target = new JsonObject();
    target.addProperty("_index", indexName);
    target.addProperty("_id", documentId);
    if (ifSeqNo != null && ifPrimaryTerm != null) {
      target.addProperty("if_seq_no", ifSeqNo);
      target.addProperty("if_primary_term", ifPrimaryTerm);
    }
    JsonObject action = new JsonObject();
    action.add(type.name().toLowerCase(), target);
    return action;
  }

  @Override
  public String toString() {
    return type + " " + documentId
        + (ifSeqNo == null ? "" : " if seq_no=" + ifSeqNo + " and primary_term=" + ifPrimaryTerm);
  }
}
