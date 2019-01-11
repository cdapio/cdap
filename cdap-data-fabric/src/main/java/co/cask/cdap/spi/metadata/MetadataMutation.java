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

package co.cask.cdap.spi.metadata;

import co.cask.cdap.api.metadata.MetadataEntity;

import java.util.Map;
import java.util.Set;

/**
 * A operation that changes metadata.
 */
public class MetadataMutation {
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
   */
  public static class Create extends MetadataMutation {
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

    public Update(Type type, MetadataEntity entity, Metadata updates) {
      super(type, entity);
      this.updates = updates;
    }

    public Metadata getUpdates() {
      return updates;
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
    private final Set<ScopedNameOfKind> removals;

    public Remove(Type type, MetadataEntity entity, Set<ScopedNameOfKind> removals) {
      super(type, entity);
      this.removals = removals;
    }

    public Set<ScopedNameOfKind> getRemovals() {
      return removals;
    }

    @Override
    public String toString() {
      return "Remove{" +
        "entity=" + getEntity() +
        "removals=" + getRemovals() +
        '}';
    }
  }
}
