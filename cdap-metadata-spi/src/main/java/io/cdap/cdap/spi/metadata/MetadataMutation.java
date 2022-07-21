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

package io.cdap.cdap.spi.metadata;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A operation that changes metadata.
 */
@Beta
public abstract class MetadataMutation {
  protected final Type type;
  protected final MetadataEntity entity;

  /**
   * Indicates the type of a mutation.
   */
  public enum Type { CREATE, DROP, UPDATE, REMOVE }

  private MetadataMutation(Type type, MetadataEntity entity) {
    this.type = type;
    this.entity = entity;
  }

  public Type getType() {
    return type;
  }

  public MetadataEntity getEntity() {
    return entity;
  }

  /**
   * Create or replace the metadata for an entity.
   *
   * For any scope that appears in the new Metadata, all existing metadata
   * is replaced according to the given directives. If a scope does not
   * occur in the Metadata, existing metadata in that scope will not be changed.
   *
   * Note that the typical use for this is the creation (or recreation) of an
   * entity, when all the entity's system metadata are defined for the first
   * time, or redefined.
   */
  public static class Create extends MetadataMutation {
    // directives for (re-)creation of system metadata:
    // - keep description if new metadata does not contain it
    // - preserve creation-time if it exists in current metadata
    public static final Map<ScopedNameOfKind, MetadataDirective> CREATE_DIRECTIVES;

    static {
      Map<ScopedNameOfKind, MetadataDirective> createDirectives = new HashMap<>();
      createDirectives.put(
        new ScopedNameOfKind(MetadataKind.PROPERTY, MetadataScope.SYSTEM, MetadataConstants.DESCRIPTION_KEY),
        MetadataDirective.KEEP);
      createDirectives.put(
        new ScopedNameOfKind(MetadataKind.PROPERTY, MetadataScope.SYSTEM, MetadataConstants.CREATION_TIME_KEY),
        MetadataDirective.PRESERVE);
      CREATE_DIRECTIVES = Collections.unmodifiableMap(createDirectives);
    }

    private final Metadata metadata;
    private final Map<ScopedNameOfKind, MetadataDirective> directives;

    public Create(MetadataEntity entity, Metadata metadata, Map<ScopedNameOfKind, MetadataDirective> directives) {
      super(Type.CREATE, entity);
      this.metadata = metadata;
      this.directives = directives;
    }

    public Metadata getMetadata() {
      return metadata;
    }

    public Map<ScopedNameOfKind, MetadataDirective> getDirectives() {
      return directives;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Create create = (Create) o;
      return Objects.equals(entity, create.entity) &&
        Objects.equals(metadata, create.metadata) &&
        Objects.equals(directives, create.directives);
    }

    @Override
    public int hashCode() {
      return Objects.hash(entity, metadata, directives);
    }

    @Override
    public String toString() {
      return "Create{" +
        "entity=" + getEntity() +
        ", metadata=" + getMetadata() +
        ", directives=" + getDirectives() +
        '}';
    }
  }

  /**
   * Drop all metadata for an entity.
   */
  public static class Drop extends MetadataMutation {
    public Drop(MetadataEntity entity) {
      super(Type.DROP, entity);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Drop drop = (Drop) o;
      return Objects.equals(entity, drop.entity);
    }

    @Override
    public int hashCode() {
      // the type is the same for all Drops, but include it here so that the hashCode is different from the entity's.
      return Objects.hash(type, entity.hashCode());
    }

    @Override
    public String toString() {
      return "Drop{" +
        "entity=" + getEntity() +
        '}';
    }
  }

  /**
   * Add or update metadata for an entity.
   */
  public static class Update extends MetadataMutation {
    private final Metadata updates;

    public Update(MetadataEntity entity, Metadata updates) {
      super(Type.UPDATE, entity);
      this.updates = updates;
    }

    public Metadata getUpdates() {
      return updates;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Update update = (Update) o;
      return Objects.equals(entity, update.entity) &&
        Objects.equals(updates, update.updates);
    }

    @Override
    public int hashCode() {
      return Objects.hash(entity, updates);
    }

    @Override
    public String toString() {
      return "Update{" +
        "entity=" + getEntity() +
        ", updates=" + getUpdates() +
        '}';
    }
  }

  /**
   * Remove metadata for an entity.
   */
  public static class Remove extends MetadataMutation {
    private final Set<MetadataScope> scopes;
    private final Set<MetadataKind> kinds;
    private final Set<ScopedNameOfKind> removals;

    /**
     * Remove all metadata for an entity.
     */
    public Remove(MetadataEntity entity) {
      this(entity, null, null, null);
    }

    /**
     * Remove a selected set of metadata for an entity.
     */
    public Remove(MetadataEntity entity, Set<ScopedNameOfKind> removals) {
      this(entity, null, null, removals);
    }

    /**
     * Remove all metadata in a given scope for an entity.
     */
    public Remove(MetadataEntity entity, MetadataScope scope) {
      this(entity, scope, null, null);
    }

    /**
     * Remove all metadata of a given kind for an entity.
     */
    public Remove(MetadataEntity entity, MetadataKind kind) {
      this(entity, null, kind, null);
    }

    /**
     * Remove all metadata of a given kind in a given scope for an entity.
     */
    public Remove(MetadataEntity entity, MetadataScope scope, MetadataKind kind) {
      this(entity, scope, kind, null);
    }

    private Remove(MetadataEntity entity, MetadataScope scope, MetadataKind kind, Set<ScopedNameOfKind> removals) {
      super(Type.REMOVE, entity);
      this.scopes = scope == null ? MetadataScope.ALL : Collections.singleton(scope);
      this.kinds = kind == null ? MetadataKind.ALL : Collections.singleton(kind);
      this.removals = removals;
    }

    public Set<ScopedNameOfKind> getRemovals() {
      return removals;
    }

    public Set<MetadataScope> getScopes() {
      return scopes;
    }

    public Set<MetadataKind> getKinds() {
      return kinds;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Remove remove = (Remove) o;
      return Objects.equals(entity, remove.entity) &&
        Objects.equals(scopes, remove.scopes) &&
        Objects.equals(kinds, remove.kinds) &&
        Objects.equals(removals, remove.removals);
    }

    @Override
    public int hashCode() {
      return Objects.hash(entity, scopes, kinds, removals);
    }

    @Override
    public String toString() {
      return "Remove{" +
        "entity=" + entity +
        ", scopes=" + scopes +
        ", kinds=" + kinds +
        ", removals=" + removals +
        '}';
    }
  }
}
