/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.proto;

import com.google.common.base.Objects;

import java.util.UUID;

/**
 * Represents a submitted query operation.
 */
public final class QueryHandle {

  private static final String NO_OP_ID = "NO_OP";
  public static final QueryHandle NO_OP = new QueryHandle(NO_OP_ID);

  private final String handle;

  public static QueryHandle generate() {
    return new QueryHandle(UUID.randomUUID().toString());
  }

  public static QueryHandle fromId(String id) {
    if (id.equals(NO_OP_ID)) {
      return NO_OP;
    }
    return new QueryHandle(id);
  }

  /**
   * Create a new QueryHandle.
   * @param handle handle of the query.
   */
  private QueryHandle(String handle) {
    this.handle = handle;
  }

  public String getHandle() {
    return handle;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("id", handle)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    QueryHandle that = (QueryHandle) o;

    return Objects.equal(this.handle, that.handle);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(handle);
  }
}
