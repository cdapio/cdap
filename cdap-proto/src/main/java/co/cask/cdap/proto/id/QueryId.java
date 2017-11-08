/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
package co.cask.cdap.proto.id;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.element.EntityType;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a query handle.
 */
public class QueryId extends EntityId {

  private final String handle;
  private transient Integer hashCode;

  public QueryId(String handle) {
    super(EntityType.QUERY);
    if (handle == null) {
      throw new NullPointerException("Query Handle cannot be null.");
    }
    this.handle = handle;
  }

  public String getHandle() {
    return handle;
  }

  @Override
  public String getEntityName() {
    return getHandle();
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    QueryId queryId = (QueryId) o;
    return Objects.equals(handle, queryId.handle);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), handle);
    }
    return hashCode;
  }

  @Override
  public Id.QueryHandle toId() {
    return Id.QueryHandle.from(handle);
  }

  @SuppressWarnings("unused")
  public static QueryId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new QueryId(nextAndEnd(iterator, "handle"));
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.singletonList(handle);
  }

  public static QueryId fromString(String string) {
    return EntityId.fromString(string, QueryId.class);
  }
}
