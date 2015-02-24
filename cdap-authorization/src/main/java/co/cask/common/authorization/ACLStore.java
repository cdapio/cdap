/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.common.authorization;

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.Set;

/**
 * Provides methods for storing and querying {@link ACLEntry}s.
 */
public interface ACLStore {

  /**
   * Writes a single {@link ACLEntry}.
   *
   *
   * @param entry the {@link ACLEntry} to write
   */
  void write(ACLEntry entry) throws Exception;

  /**
   * Checks for the existence of an {@link ACLEntry}.
   *
   *
   * @param entry the {@link co.cask.common.authorization.ACLEntry} to check
   * @return true if the {@link ACLEntry} exists
   */
  boolean exists(ACLEntry entry) throws Exception;

  /**
   * Deletes an {@link ACLEntry} matching.
   *
   *
   * @param entry the {@link ACLEntry} to delete
   */
  void delete(ACLEntry entry) throws Exception;

  /**
   * Fetches {@link ACLEntry}s matching the specified {@link ACLStore.Query}.
   *
   * @param query specifies the {@link ACLEntry}s to read
   * @return the {@link ACLEntry}s that have the {@code object}.
   */
  Set<ACLEntry> search(Iterable<Query> query) throws Exception;

  /**
   * Deletes {@link ACLEntry}s matching the specified {@link ACLStore.Query}.
   *
   * @param query specifies the {@link ACLEntry}s to delete
   */
  void delete(Iterable<Query> query) throws Exception;

  /**
   * Represents a query to match when searching for ACLs. Null implies match anything.
   */
  public static final class Query implements Iterable<Query> {

    private final ObjectId objectId;
    private final SubjectId subjectId;
    private final Permission permission;

    public Query(ObjectId objectId, SubjectId subjectId, Permission permission) {
      this.objectId = objectId;
      this.subjectId = subjectId;
      this.permission = permission;
    }

    public Query(ObjectId objectId, SubjectId subjectId) {
      this(objectId, subjectId, null);
    }

    public Query(ACLEntry entry) {
      this(entry.getObject(), entry.getSubject(), entry.getPermission());
    }

    public ObjectId getObjectId() {
      return objectId;
    }

    public SubjectId getSubjectId() {
      return subjectId;
    }

    public Permission getPermission() {
      return permission;
    }

    /**
     * @param entry the entry to check
     * @return true if entry matches this condition
     */
    public boolean matches(ACLEntry entry) {
      return !(objectId != null && !objectId.equals(entry.getObject()))
        && !(subjectId != null && !subjectId.equals(entry.getSubject()))
        && !(permission != null && !permission.equals(entry.getPermission()));

    }

    @Override
    public Iterator<Query> iterator() {
      return Iterators.singletonIterator(this);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("objectId", objectId)
        .add("subjectId", subjectId)
        .add("permission", permission)
        .toString();
    }
  }
}
