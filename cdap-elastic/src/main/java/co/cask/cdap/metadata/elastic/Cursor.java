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

package co.cask.cdap.metadata.elastic;

/**
 * A cursor for an Elasticsearch search session.
 * It contains the Elasticsearch scroll id, the offset, and the page size.
 */
class Cursor {
  private final int offset;
  private final int pageSize;
  private final String scrollId;

  Cursor(int offset, int pageSize, String scrollId) {
    this.offset = offset;
    this.pageSize = pageSize;
    this.scrollId = scrollId;
  }

  int getOffset() {
    return offset;
  }

  int getPageSize() {
    return pageSize;
  }

  String getScrollId() {
    return scrollId;
  }

  @Override
  public String toString() {
    return String.format("%d:%d:%s", offset, pageSize, scrollId);
  }

  /**
   * Deserialize a cursor from the same same string representation as
   * produced by {@link #toString()}, that is, "{offset}:{page size}:{scroll id}".
   */
  static Cursor fromString(String str) {
    String[] parts = str.split(":", 3);
    if (parts.length != 3) {
      throw new IllegalArgumentException(
        String.format("Unable to parse cursor '%s': it must be of the form 'offset:pageSize:scrollId'", str));
    }
    int offset;
    try {
      offset = Integer.parseInt(parts[0]);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
        String.format("Unable to parse cursor '%s': the first part is not a valid integer", str), e);
    }
    int pageSize;
    try {
      pageSize = Integer.parseInt(parts[1]);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
        String.format("Unable to parse cursor '%s': the second part is not a valid integer", str), e);
    }
    return new Cursor(offset, pageSize, parts[2]);
  }

  /**
   * An elasticsearch scroll inherits the page size of the original request,
   * and no offset relative to the scroll is supported. This validates
   * that a new search request matches the page size of the cursor.
  */
  void validate(int offset, int limit) {
    if (offset != 0) {
      throw new IllegalArgumentException("Offset must 0 for search requests that have a cursor");
    }
    if (limit != this.pageSize) {
      throw new IllegalArgumentException("Page size must be same as the page size of the original request");
    }
  }
}
