/*
 * Copyright © 2015-2021 Cask Data, Inc.
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
package io.cdap.cdap.proto.id;

import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.proto.element.EntityType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
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
 *   </ol>
 * </p>
 */
@SuppressWarnings("unchecked")
public abstract class EntityId {

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
  /**
   * Estimate of maximum height of entity tree (max length of {@link #getHierarchy()}
   */
  public static final int ENTITY_TREE_MAX_HEIGHTS = 5;

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
  // Hierarchy should be transient since it acts like a cache variable and should not be JSON serialized.
  private transient volatile Deque<EntityId> hierarchy;

  protected EntityId(EntityType entity) {
    if (entity == null) {
      throw new NullPointerException("Entity type cannot be null.");
    }
    this.entity = entity;
  }

  public abstract Iterable<String> toIdParts();

  public abstract String getEntityName();

  /**
   * @return The {@link MetadataEntity} which represents this {@link EntityId}. If an Entity supports metadata the
   * Entityid of the entity must override this method and provide the correct implementation to convert the EntityId
   * to {@link MetadataEntity}
   * @throws UnsupportedOperationException if the entityId does not support conversion to {@link MetadataEntity}
   */
  public MetadataEntity toMetadataEntity() {
    throw new UnsupportedOperationException("Metadata is not supported");
  }

  /**
   * Returns the EntityId represented by the given MetadataEntity. Note: Custom MetadataEntity cannot be converted
   * into EntityId hence this call is only safe to be called if the MetadataEntity does represent an EntityId and can
   * actually be converted to an EntityId.
   * @param metadataEntity the MetadataEntity which needs to be converted to EntityId
   * @return the EntityId
   * @throws IllegalArgumentException if the metadataEntity does not represent an EntityId and is a custom
   * metadataEntity.
   */
  public static <T extends EntityId> T fromMetadataEntity(MetadataEntity metadataEntity) {
    // check that the type of teh metadata entity is known type
    EntityType.valueOf(metadataEntity.getType().toUpperCase());
    return getSelfOrParentEntityId(metadataEntity);
  }

  /**
   * Creates a valid known CDAP entity which can be considered as the parent for the MetadataEntity by walking up the
   * key-value hierarchy of the MetadataEntity till a known CDAP {@link EntityType} is found. If the last node itself
   * is known type then that will be considered as the parent.
   *
   * @param metadataEntity whose parent entityId needs to be found
   * @return {@link EntityId} of the given metadataEntity
   * @throws IllegalArgumentException if the metadataEntity does not have any know entity type in it's hierarchy or if
   * it does not have all the required key-value pairs to construct the identified EntityId.
   */
  public static <T extends EntityId> T getSelfOrParentEntityId(MetadataEntity metadataEntity) {
    EntityType entityType = findParentType(metadataEntity);
    if (entityType == null) {
      throw new IllegalArgumentException(String.format("No known type found in the hierarchy of %s", metadataEntity));
    }
    List<String> values = new LinkedList<>();
    // get the key-value pair till the known entity-type. Note: for application the version comes after application
    // key-value pair and needs to be included too
    List<MetadataEntity.KeyValue> extractedParts = metadataEntity.head(entityType.toString());
    // if a version was specified extract that else use the default version
    String version = metadataEntity.containsKey(MetadataEntity.VERSION) ?
      metadataEntity.getValue(MetadataEntity.VERSION) : ApplicationId.DEFAULT_VERSION;
    if (entityType == EntityType.APPLICATION) {
      // if the entity is an application our extractParts will not contain the version info since we extracted till
      // application so append it
      extractedParts.add(new MetadataEntity.KeyValue(MetadataEntity.VERSION, version));
    }
    if (entityType == EntityType.PROGRAM || entityType == EntityType.SCHEDULE || entityType == EntityType.PROGRAM_RUN) {
      // if the entity is program or schedule then add the version information at its correct position i.e. 2
      // (namespace, application, version) if the version information is not present
      if (!metadataEntity.containsKey(MetadataEntity.VERSION)) {
        extractedParts.add(2, new MetadataEntity.KeyValue(MetadataEntity.VERSION, version));
      }
    }
    // for artifacts get till version (artifacts always have version
    if (entityType == EntityType.ARTIFACT) {
      extractedParts = metadataEntity.head(MetadataEntity.VERSION);
    }

    // for plugins get till plugin name
    if (entityType == EntityType.PLUGIN) {
      extractedParts = metadataEntity.head(MetadataEntity.PLUGIN);
    }
    extractedParts.iterator().forEachRemaining(keyValue -> values.add(keyValue.getValue()));
    return entityType.fromIdParts(values);
  }

  /**
   * Finds a valid known CDAP entity which can be considered as the parent for the MetadataEntity by walking up the
   * key-value hierarchy of the MetadataEntity
   *
   * @param metadataEntity whose EntityType needs to be determined
   * @return {@link EntityType} of the given metadataEntity
   */
  @Nullable
  private static EntityType findParentType(MetadataEntity metadataEntity) {
    List<String> keys = new ArrayList<>();
    metadataEntity.getKeys().forEach(keys::add);
    int curIndex = keys.size() - 1;
    EntityType entityType = null;
    while (curIndex >= 0) {
      try {
        entityType = EntityType.valueOf(keys.get(curIndex).toUpperCase());
        // found a valid entity type;
        break;
      } catch (IllegalArgumentException e) {
        // the current key is not a valid cdap entity type so try up in hierarchy
        curIndex--;
      }
    }
    return entityType;
  }

  public final EntityType getEntityType() {
    return entity;
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
    return getHierarchy(false);
  }

  public Iterable<EntityId> getHierarchy(boolean reverse) {
    if (hierarchy == null) {
      Deque<EntityId> hierarchy = new ArrayDeque<>(ENTITY_TREE_MAX_HEIGHTS);
      EntityId current = this;
      while (current instanceof ParentedId) {
        hierarchy.addFirst(current);
        current = ((ParentedId) current).getParent();
      }
      hierarchy.addFirst(current);
      this.hierarchy = hierarchy;
    }
    return reverse ? hierarchy::descendingIterator : hierarchy;
  }
}
