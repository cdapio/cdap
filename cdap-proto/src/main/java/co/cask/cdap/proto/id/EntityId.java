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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
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
 *         <li>{@link NamespacedEntityId} if the new ID belongs to a namespace</li>
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

  public static final String IDSTRING_TYPE_SEPARATOR = ":";
  public static final String IDSTRING_PART_SEPARATOR = ".";
  public static final Pattern IDSTRING_PART_SEPARATOR_PATTERN = Pattern.compile("\\.");

  // Allow hyphens for other ids.
  private static final Pattern idPattern = Pattern.compile("[a-zA-Z0-9_-]+");
  // Allow '.' and hyphens for artifact ids.
  private static final Pattern artifactIdPattern = Pattern.compile("[\\.a-zA-Z0-9_-]+");
  // Allow '.' and '$' for dataset ids since they can be fully qualified class names
  private static final Pattern datasetIdPattern = Pattern.compile("[$\\.a-zA-Z0-9_-]+");
  // Only allow alphanumeric and _ character for namespace
  private static final Pattern namespacePattern = Pattern.compile("[a-zA-Z0-9_]+");
  // Allow '.' for versionId
  private static final Pattern versionIdPattern = Pattern.compile("[\\.a-zA-Z0-9_-]+");

  public static void ensureValidId(String propertyName, String name) {
    if (!isValidId(name)) {
      throw new IllegalArgumentException(String.format("Invalid %s ID: %s. Should only contain alphanumeric " +
                                                         "characters and _ or -.", propertyName, name));
    }
  }

  public static void ensureValidArtifactId(String propertyName, String name) {
    if (!isValidArtifactId(name)) {
      throw new IllegalArgumentException(String.format("Invalid %s ID: %s. Should only contain alphanumeric " +
                                                         "characters and _ or - or .", propertyName, name));
    }
  }

  public static boolean isValidId(String name) {
    return idPattern.matcher(name).matches();
  }

  public static boolean isValidArtifactId(String name) {
    return artifactIdPattern.matcher(name).matches();
  }


  public static void ensureValidDatasetId(String propertyName, String datasetId) {
    if (!isValidDatasetId(datasetId)) {
      throw new IllegalArgumentException(String.format("Invalid %s ID: %s. Should only contain alphanumeric " +
                                                         "characters, $, ., _, or -.", propertyName, datasetId));
    }
  }

  public static boolean isValidDatasetId(String datasetId) {
    return datasetIdPattern.matcher(datasetId).matches();
  }

  public static void ensureValidNamespace(String namespace) {
    if (!isValidNamespace(namespace)) {
      throw new IllegalArgumentException(String.format("Invalid namespace ID: %s. Should only contain alphanumeric " +
                                                         "characters or _.", namespace));
    }
  }

  public static boolean isValidNamespace(String namespace) {
    return namespacePattern.matcher(namespace).matches();
  }

  public static boolean isValidVersionId(String datasetId) {
    return versionIdPattern.matcher(datasetId).matches();
  }

  private final EntityType entity;
  private Vector<EntityId> hierarchy;

  protected EntityId(EntityType entity) {
    if (entity == null) {
      throw new NullPointerException("Entity type cannot be null.");
    }
    this.entity = entity;
  }

  public abstract Iterable<String> toIdParts();

  public abstract String getEntityName();

  public final EntityType getEntityType() {
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
    EntityType typeFromString = EntityType.valueOf(typeString.toUpperCase());
    Class<T> idClassFromString = (Class<T>) typeFromString.getIdClass();
    if (idClass != null && !idClassFromString.equals(idClass)) {
      /* idClass can differ from idClassFromString only when typeFromString is EntityType.PROGRAM and idClass is
       * of WorkflowId.class, because when toString method is called for WorkflowId, its superclass ProgramId calls
       * EntityId's toString() method and the string always contains ProgramId as type. */
      if (!idClassFromString.isAssignableFrom(idClass) || !typeFromString.equals(EntityType.PROGRAM)) {
        throw new IllegalArgumentException(String.format("Expected EntityId of class '%s' but got '%s'",
                                                         idClass.getName(), typeFromString.getIdClass().getName()));
      }
    }
    String idString = typeAndId[1];
    try {
      List<String> idParts = Arrays.asList(IDSTRING_PART_SEPARATOR_PATTERN.split(idString));
      // special case for DatasetId, DatasetModuleId, DatasetTypeId since we allow . in the name
      if (EnumSet.of(EntityType.DATASET, EntityType.DATASET_MODULE, EntityType.DATASET_TYPE).contains(typeFromString)) {
        int namespaceSeparatorPos = idString.indexOf(IDSTRING_PART_SEPARATOR);
        if (namespaceSeparatorPos > 0) {
          idParts = new ArrayList<>();
          idParts.add(idString.substring(0, namespaceSeparatorPos));
          idParts.add(idString.substring(namespaceSeparatorPos + 1));
        }
      }
      return typeFromString.fromIdParts(idParts);
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
    if (!(o instanceof EntityId)) {
      return false;
    }
    EntityId entityId = (EntityId) o;
    return entity == entityId.entity;
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
