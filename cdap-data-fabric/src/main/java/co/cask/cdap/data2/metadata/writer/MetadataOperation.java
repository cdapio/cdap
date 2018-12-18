/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.writer;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a meta data operation for an entity.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public abstract class MetadataOperation {

  /**
   * Type of the operation:
   * <ul><li>
   * {@link Type#CREATE} to add system metadata, with optional properties for new entities
   * </li><li>
   * {@link Type#DROP} to delete all metadata (properties and tags) in all scopes
   * </li><li>
   * {@link Type#PUT} for adding metadata in a scope
   * </li><li>
   * {@link Type#DELETE} for removing metadata in a scope.
   * </li><li>
   * {@link Type#DELETE_ALL} for removing all metadata in a scope.
   * </li><li>
   * {@link Type#DELETE_ALL_PROPERTIES} for removing all properties in a scope.
   * </li><li>
   * {@link Type#DELETE_ALL_TAGS} for removing all tags in a scope.
   * </li></ul>
   */
  public enum Type {
    CREATE, DROP, PUT, DELETE, DELETE_ALL, DELETE_ALL_PROPERTIES, DELETE_ALL_TAGS
  }

  protected final Type type;
  protected final MetadataEntity entity;

  /**
   * @param type The operation type.
   * @param entity The metadata entity that this operation applies to
   */
  private MetadataOperation(Type type, MetadataEntity entity) {
    this.type = type;
    this.entity = entity;
  }

  public Type getType() {
    return type;
  }

  public MetadataEntity getEntity() {
    return entity;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetadataOperation that = (MetadataOperation) o;
    return Objects.equals(entity, that.entity) && type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(entity, type);
  }

  /**
   * Represents the creation or update of an entity. Similar to a {@link Put} with
   * an additional set of properties that are only added if the entity did not exist
   * yet (that is, if these properties are not defined yet). This is useful, for
   * example, for properties like the creation date, which should never change when
   * an entity is modified.
   */
  public static class Create extends MetadataOperation {
    private final Map<String, String> properties;
    private final Set<String> tags;

    /**
     * @param entity The metadata entity that this operation applies to
     * @param properties The properties to be deleted
     * @param tags The names of the tags to be deleted
     */
    public Create(MetadataEntity entity, Map<String, String> properties, Set<String> tags) {
      super(Type.CREATE, entity);
      this.properties = ImmutableMap.copyOf(properties);
      this.tags = ImmutableSet.copyOf(tags);
    }

    public Map<String, String> getProperties() {
      return properties;
    }

    public Set<String> getTags() {
      return tags;
    }

    @Override
    public boolean equals(Object o) {
      if (!super.equals(o)) {
        return false;
      }
      Create that = (Create) o;
      return Objects.equals(properties, that.properties)
        && Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), properties, tags);
    }

    @Override
    public String toString() {
      return "Create{" +
        "type=" + type +
        ", entity=" + entity +
        ", properties=" + properties +
        ", tags=" + tags +
        '}';
    }
  }

  /**
   * Represents the deletion of an entity, which results in removing all it from
   * the metadata store, including all its tags and properties.
   */
  public static class Drop extends MetadataOperation {

    /**
     * @param entity The metadata entity that this operation applies to
     */
    public Drop(MetadataEntity entity) {
      super(Type.DROP, entity);
    }

    @Override
    public String toString() {
      return "Drop{" +
        "type=" + type +
        ", entity=" + entity +
        '}';
    }
  }

  /**
   * A metadata operation that only applies to one scope.
   */
  private abstract static class ScopedOperation extends MetadataOperation {
    protected final MetadataScope scope;

    /**
     * @param type The operation type.
     * @param entity The metadata entity that this operation applies to
     * @param scope The scope in which this operation applies
     */
    private ScopedOperation(Type type, MetadataEntity entity, MetadataScope scope) {
      super(type, entity);
      this.scope = scope;
    }

    public MetadataScope getScope() {
      return scope;
    }

    @Override
    public boolean equals(Object o) {
      return super.equals(o) && scope == ((ScopedOperation) o).scope;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), scope);
    }
  }

  /**
   * Represents addition or update of metadata.
   */
  public static class Put extends ScopedOperation {
    private final Map<String, String> properties;
    private final Set<String> tags;

    /**
     * @param entity The metadata entity that this operation applies to
     * @param properties The properties to be deleted
     * @param tags The names of the tags to be deleted
     */
    public Put(MetadataEntity entity, Map<String, String> properties, Set<String> tags) {
      this(entity, MetadataScope.USER, properties, tags);
    }

    /**
     * @param entity The metadata entity that this operation applies to
     * @param scope The scope in which this operation applies
     * @param properties The properties to be deleted
     * @param tags The names of the tags to be deleted
     */
    public Put(MetadataEntity entity, MetadataScope scope, Map<String, String> properties, Set<String> tags) {
      super(Type.PUT, entity, scope);
      this.properties = ImmutableMap.copyOf(properties);
      this.tags = ImmutableSet.copyOf(tags);
    }

    public Map<String, String> getProperties() {
      return properties;
    }

    public Set<String> getTags() {
      return tags;
    }

    @Override
    public boolean equals(Object o) {
      if (!super.equals(o)) {
        return false;
      }
      Put that = (Put) o;
      return Objects.equals(properties, that.properties)
        && Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), properties, tags);
    }

    @Override
    public String toString() {
      return "Put{" +
        "type=" + type +
        ", entity=" + entity +
        ", scope=" + scope +
        ", properties=" + properties +
        ", tags=" + tags +
        '}';
    }
  }

  /**
   * Represents a deletion of metadata.
   */
  public static class Delete extends ScopedOperation {
    private final Set<String> properties;
    private final Set<String> tags;

    /**
     * Convenience constructor for scope USER.
     *
     * @param entity The metadata entity that this operation applies to
     * @param properties The names of the properties to be deleted
     * @param tags The names of the tags to be deleted
     */
    public Delete(MetadataEntity entity, Set<String> properties, Set<String> tags) {
      this(entity, MetadataScope.USER, properties, tags);
    }

    /**
     * @param entity The metadata entity that this operation applies to
     * @param scope The scope in which this operation applies
     * @param properties The names of the properties to be deleted
     * @param tags The names of the tags to be deleted
     */
    public Delete(MetadataEntity entity, MetadataScope scope, Set<String> properties, Set<String> tags) {
      super(Type.DELETE, entity, scope);
      this.properties = ImmutableSet.copyOf(properties);
      this.tags = ImmutableSet.copyOf(tags);
    }

    public Set<String> getProperties() {
      return properties;
    }

    public Set<String> getTags() {
      return tags;
    }

    @Override
    public boolean equals(Object o) {
      if (!super.equals(o)) {
        return false;
      }
      Delete that = (Delete) o;
      return Objects.equals(properties, that.properties)
        && Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), properties, tags);
    }

    @Override
    public String toString() {
      return "Delete{" +
        "type=" + type +
        ", entity=" + entity +
        ", scope=" + scope +
        ", properties=" + properties +
        ", tags=" + tags +
        '}';
    }
  }

  /**
   * Deletion of all metadata for an entity in a given scope.
   */
  public static class DeleteAll extends ScopedOperation {

    /**
     * Convenience constructor for scope USER.
     *
     * @param entity The metadata entity that this operation applies to
     */
    public DeleteAll(MetadataEntity entity) {
      this(entity, MetadataScope.USER);
    }

    /**
     * @param entity The metadata entity that this operation applies to
     * @param scope The scope in which this operation applies
     */
    public DeleteAll(MetadataEntity entity, MetadataScope scope) {
      super(Type.DELETE_ALL, entity, scope);
    }

    @Override
    public String toString() {
      return "DeleteAll{" +
        "type=" + type +
        ", entity=" + entity +
        ", scope=" + scope +
        '}';
    }
  }

  /**
   * Deletion of all properties for an entity in a given scope.
   */
  public static class DeleteAllProperties extends ScopedOperation {

    /**
     * Convenience constructor for scope USER.
     *
     * @param entity The metadata entity that this operation applies to
     */
    public DeleteAllProperties(MetadataEntity entity) {
      this(entity, MetadataScope.USER);
    }

    /**
     * @param entity The metadata entity that this operation applies to
     * @param scope The scope in which this operation applies
     */
    public DeleteAllProperties(MetadataEntity entity, MetadataScope scope) {
      super(Type.DELETE_ALL_PROPERTIES, entity, scope);
    }

    @Override
    public String toString() {
      return "DeleteAllProperties{" +
        "type=" + type +
        ", entity=" + entity +
        ", scope=" + scope +
        '}';
    }
  }

  /**
   * Deletion of all tags for an entity in a given scope.
   */
  public static class DeleteAllTags extends ScopedOperation {

    /**
     * Convenience constructor for scope USER.
     *
     * @param entity The metadata entity that this operation applies to
     */
    public DeleteAllTags(MetadataEntity entity) {
      this(entity, MetadataScope.USER);
    }

    /**
     * @param entity The metadata entity that this operation applies to
     * @param scope The scope in which this operation applies
     */
    public DeleteAllTags(MetadataEntity entity, MetadataScope scope) {
      super(Type.DELETE_ALL_TAGS, entity, scope);
    }

    @Override
    public String toString() {
      return "DeleteAllTags{" +
        "type=" + type +
        ", entity=" + entity +
        ", scope=" + scope +
        '}';
    }
  }
}
