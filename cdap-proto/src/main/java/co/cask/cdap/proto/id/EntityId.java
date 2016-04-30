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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.Vector;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Uniquely identifies a particular instance of an element.
 *
 * <p>
 *   When adding a new type of {@link EntityId}, the following must be done:
 *   <ol>
 *     <li>
 *       Implement interfaces
 *       <ol>
 *         <li>{@link NamespacedId} if the new ID belongs to a namespace</li>
 *         <li>{@link ParentedId} if the new ID has a parent ID</li>
 *       </ol>
 *     </li>
 *     <li>
 *       Add methods
 *       <ol>
 *         <li>{@code equals()} and {@code hashCode()}, using the implementations in {@link EntityId}</li>
 *         <li>{@code fromString(String string)}, delegating to {@link EntityId#fromString(String, Class)}</li>
 *       </ol>
 *     </li>
 *     <li>
 *       Create a corresponding child class of the old {@link Id}
 *       (once {@link Id} is removed, this is no longer needed)
 *     </li>
 *     <li>
 *       Add a new entry to {@link EntityType}, associating the {@link EntityType}
 *       with both the new {@link EntityId} and old {@link Id}
 *     </li>
 *   </ol>
 * </p>
 */
@SuppressWarnings("unchecked")
public abstract class EntityId implements IdCompatible {

  private static final String IDSTRING_TYPE_SEPARATOR = ":";
  private static final String IDSTRING_PART_SEPARATOR = ".";
  private static final Pattern IDSTRING_PART_SEPARATOR_PATTERN = Pattern.compile("\\.");

  // Only allow alphanumeric and _ character for namespace
  private static final Pattern namespacePattern = Pattern.compile("[a-zA-Z0-9_]+");
  // Allow hyphens for other ids.
  private static final Pattern idPattern = Pattern.compile("[a-zA-Z0-9_-]+");
  // Allow '.' and '$' for dataset ids since they can be fully qualified class names
  private static final Pattern datasetIdPattern = Pattern.compile("[$\\.a-zA-Z0-9_-]+");

  protected static boolean isValidNamespaceId(String name) {
    return namespacePattern.matcher(name).matches();
  }

  protected static boolean isValidId(String name) {
    return idPattern.matcher(name).matches();
  }

  protected static boolean isValidDatasetId(String datasetId) {
    return datasetIdPattern.matcher(datasetId).matches();
  }

  private final EntityType entity;
  private Vector<EntityId> hierarchy;

  protected EntityId(EntityType entity) {
    this.entity = entity;
  }

  protected abstract Iterable<String> toIdParts();

  public final EntityType getEntity() {
    return entity;
  }

  public static <T extends Id> T fromStringOld(String string, Class<T> oldIdClass) {
    EntityType type = EntityType.valueOfOldIdClass(oldIdClass);
    EntityId id = fromString(string, type.getIdClass());
    return (T) id.toId();
  }

  public static <T extends EntityId> T fromString(String string) {
    return fromString(string, null);
  }

  protected static <T extends EntityId> T fromString(String string, @Nullable Class<T> idClass) {
    String[] typeAndId = string.split(IDSTRING_TYPE_SEPARATOR, 2);
    if (typeAndId.length != 2) {
      throw new IllegalArgumentException(
        String.format("Expected type separator '%s' to be in the ID string: %s", IDSTRING_TYPE_SEPARATOR, string));
    }

    String typeString = typeAndId[0];
    EntityType type = EntityType.valueOf(typeString.toUpperCase());
    if (type == null) {
      throw new IllegalArgumentException("Invalid element type: " + typeString);
    }
    if (idClass != null && !type.getIdClass().equals(idClass)) {
        throw new IllegalArgumentException(String.format("Expected EntityId of class '%s' but got '%s'",
                                                         idClass.getName(), type.getIdClass().getName()));
    }
    String idString = typeAndId[1];
    try {
      return type.fromIdParts(Arrays.asList(IDSTRING_PART_SEPARATOR_PATTERN.split(idString)));
    } catch (IllegalArgumentException e) {
      String message = idClass == null ? String.format("Invalid ID: %s", string) :
        String.format("Invalid ID for type '%s': %s", idClass.getName(), string);
      throw new IllegalArgumentException(message, e);
    }
  }

  @Override
  public final String toString() {
    StringBuilder result = new StringBuilder();
    result.append(entity.name().toLowerCase());

    String separator = IDSTRING_TYPE_SEPARATOR;
    for (String part : toIdParts()) {
      result.append(separator).append(part);
      separator = IDSTRING_PART_SEPARATOR;
    }
    return result.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EntityId entityId = (EntityId) o;
    return Objects.equals(entity, entityId.entity);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entity);
  }

  protected static String next(Iterator<String> iterator, String fieldName) {
    if (!iterator.hasNext()) {
      throw new IllegalArgumentException("Missing field: " + fieldName);
    }
    return iterator.next();
  }

  protected static String nextAndEnd(Iterator<String> iterator, String fieldName) {
    String result = next(iterator, fieldName);
    if (iterator.hasNext()) {
      throw new IllegalArgumentException(
        String.format("Expected end after field '%s' but got: %s", fieldName, remaining(iterator)));
    }
    return result;
  }

  protected static String remaining(Iterator<String> iterator, @Nullable String fieldName) {
    if (fieldName != null && !iterator.hasNext()) {
      throw new IllegalArgumentException("Missing field: " + fieldName);
    }
    StringBuilder result = new StringBuilder();
    String separator = "";
    while (iterator.hasNext()) {
      result.append(separator).append(iterator.next());
      separator = IDSTRING_PART_SEPARATOR;
    }
    return result.toString();
  }

  private static String remaining(Iterator<String> iterator) {
    return remaining(iterator, null);
  }

  public Iterable<EntityId> getHierarchy() {
    if (hierarchy == null) {
      Vector<EntityId> hierarchy = new Vector<>();
      EntityId current = this;
      while (current instanceof ParentedId) {
        hierarchy.insertElementAt(current, 0);
        current = ((ParentedId) current).getParent();
      }
      hierarchy.insertElementAt(current, 0);
      this.hierarchy = hierarchy;
    }
    return hierarchy;
  }
}
