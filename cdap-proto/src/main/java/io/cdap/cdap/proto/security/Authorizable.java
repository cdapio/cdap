/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
package io.cdap.cdap.proto.security;


import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Class to represent entities on which privileges can be granted/revoked.
 * This class allows wildcard privileges (* and ?).
 */
public class Authorizable {
  // represents the type of entity
  private final EntityType entityType;
  // represent the parts of the entity
  private final Map<EntityType, String> entityParts;
  // represents the type of children for parent-only permission checks
  private final EntityType childType;


  public Authorizable(EntityType entityType, Map<EntityType, String> entityParts, @Nullable EntityType childType) {
    this.entityType = entityType;
    this.entityParts = Collections.unmodifiableMap(entityParts);
    this.childType = childType;
  }

  /**
   * Constructs an {@link Authorizable} from the given entityString. The entityString must be a representation of an
   * entity similar to {@link EntityId#toString()} with the exception that the string can contain wildcards (? and *).
   * ChildType can be optionally specified after a second ":" character.
   * Note:
   * <ol>
   * <li>
   * The only validation that this class performs on the entityString is that it checks if the string has enough
   * valid parts for the given {@link #entityType}. It does not check if the entity exists or not. This is
   * required to allow pre grants.
   * </li>
   * <li>
   * CDAP Authorization does not support authorization on versions of {@link io.cdap.cdap.proto.id.ApplicationId}
   * and {@link io.cdap.cdap.proto.id.ArtifactId}. If a version is included while construction an Authorizable
   * through {@link #fromString(String)} an {@link IllegalArgumentException} will be thrown.
   * </li>
   * </ol>
   *
   * @param entityString the {@link EntityId#toString()} of the entity which may or may not contains wildcards(? or *)
   */
  public static Authorizable fromString(String entityString) {
    if (entityString == null || entityString.isEmpty()) {
      throw new IllegalArgumentException("Null or empty entity string.");
    }

    String[] typeAndId = entityString.split(EntityId.IDSTRING_TYPE_SEPARATOR, 3);
    if (typeAndId.length < 2) {
      throw new IllegalArgumentException(
        String.format("Cannot extract the entity type from %s", entityString));
    }
    String typeString = typeAndId[0];
    EntityType type;
    // since Authorizable is versionless, applicationRef and programRef should be the same type as
    // application and program
    switch (typeString) {
      case "applicationreference":
        type = EntityType.valueOf("APPLICATION");
        break;
      case "programreference":
        type = EntityType.valueOf("PROGRAM");
        break;
      default:
        type = EntityType.valueOf(typeString.toUpperCase());
    }
    String idString = typeAndId[1];
    EntityType childType = typeAndId.length == 3 ? EntityType.valueOf(typeAndId[2].toUpperCase()) : null;

    List<String> idParts = Collections.emptyList();
    switch (type) {
      case KERBEROSPRINCIPAL:
        // kerberos principal might contain . which is also EntityId.IDSTRING_PART_SEPARATOR_PATTERN so don't split for
        // them and also principal doesn't have other parts so just initialize
        idParts = Collections.singletonList(idString);
        break;
      case DATASET:
      case DATASET_TYPE:
      case DATASET_MODULE:
        int namespaceSeparatorPos = idString.indexOf(EntityId.IDSTRING_PART_SEPARATOR);
        if (namespaceSeparatorPos > 0) {
          idParts = new ArrayList<>();
          idParts.add(idString.substring(0, namespaceSeparatorPos));
          idParts.add(idString.substring(namespaceSeparatorPos + 1));
        }
        break;
      default:
        idParts = Arrays.asList(EntityId.IDSTRING_PART_SEPARATOR_PATTERN.split(idString));
    }

    Map<EntityType, String> entityParts = new LinkedHashMap<>();
    checkParts(type, idParts, idParts.size() - 1, entityParts);
    return new Authorizable(type, entityParts, childType);
  }

  /**
   * Creates an {@link Authorizable} which represents the given entityId.
   * Note: CDAP Authorization does not support authorization on versions of {@link io.cdap.cdap.proto.id.ApplicationId}
   * and {@link io.cdap.cdap.proto.id.ArtifactId}. If an artifactId or applicationId which has version is passed to
   * then the version will be silently dropped to construct the authorizable.
   *
   * @param entityId the entity
   * @return {@link Authorizable} representing the entity
   */
  public static Authorizable fromEntityId(EntityId entityId) {
    return fromEntityId(entityId, null);
  }
  /**
   * Creates an {@link Authorizable} which represents the given entityId with an optional child type for parent-only
   * checks.
   * Note: CDAP Authorization does not support authorization on versions of {@link io.cdap.cdap.proto.id.ApplicationId}
   * and {@link io.cdap.cdap.proto.id.ArtifactId}. If an artifactId or applicationId which has version is passed to
   * then the version will be silently dropped to construct the authorizable.
   *
   * @param entityId the entity
   * @param childType optional type of child for parent-only checks
   * @return {@link Authorizable} representing the entity
   */
  public static Authorizable fromEntityId(EntityId entityId, @Nullable EntityType childType) {
    if (entityId == null) {
      throw new IllegalArgumentException("EntityId is required.");
    }
    String entity = entityId.toString();
    // drop the version for artifact or application
    if (entityId.getEntityType().equals(EntityType.ARTIFACT) ||
      entityId.getEntityType().equals(EntityType.APPLICATION) || entityId.getEntityType().equals(EntityType.PROGRAM)) {
      int versionStartIndex = entity.indexOf(EntityId.IDSTRING_PART_SEPARATOR,
                                             entity.indexOf(EntityId.IDSTRING_PART_SEPARATOR) + 1);
      int versionEndIndex = entity.length();
      if (entityId.getEntityType().equals(EntityType.PROGRAM)) {
        // for programs versions doesn't end at the end of the entity string as there is program type and name
        // afterwards
        versionEndIndex = entity.lastIndexOf(EntityId.IDSTRING_PART_SEPARATOR,
                                             entity.lastIndexOf(EntityId.IDSTRING_PART_SEPARATOR) - 1);
      }
      // remove the version from the entity string
      String version = entity.substring(versionStartIndex, versionEndIndex);
      entity = entity.replace(version, "");
    }
    if (childType != null) {
      entity = entity + EntityId.IDSTRING_TYPE_SEPARATOR + childType.name().toLowerCase();
    }
    return fromString(entity);
  }

  /**
   * @return the type of the entity which is represented by this authorizable
   */
  public EntityType getEntityType() {
    return entityType;
  }

  /**
   * @return a map which represents the parts of the entity in ordered according to CDAP entity hierarchy. For
   * example: Dataset comes after Namespace, Program comes after an Application etc. according to CDAP entity hierarchy.
   */
  public Map<EntityType, String> getEntityParts() {
    return Collections.unmodifiableMap(entityParts);
  }

  /**
   * @return the type of children for parent-only permission checks
   */
  public EntityType getChildType() {
    return childType;
  }

  /**
   * @return a string representation of the authorizable which is compatible with {@link EntityId#toString()}.
   */
  @Override
  public String toString() {
    // the to string is done in this way to maintain compatibility with EntityId.toString()
    StringBuilder result = new StringBuilder();
    result.append(entityType.name().toLowerCase());

    String separator = EntityId.IDSTRING_TYPE_SEPARATOR;
    for (String part : entityParts.values()) {
      result.append(separator).append(part);
      separator = EntityId.IDSTRING_PART_SEPARATOR;
    }
    if (childType != null) {
      result.append(EntityId.IDSTRING_TYPE_SEPARATOR).append(childType.name().toLowerCase());
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

    Authorizable that = (Authorizable) o;
    return entityType == that.entityType && childType == that.childType && entityParts.equals(that.entityParts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entityType, childType, entityParts);
  }

  private static void checkParts(EntityType entityType, List<String> parts, int index,
                                 Map<EntityType, String> entityParts) {
    switch (entityType) {
      case INSTANCE:
      case SYSTEM_SERVICE:
      case NAMESPACE:
      case KERBEROSPRINCIPAL:
        if (parts.size() != 1 && index == (parts.size() - 1)) {
          throw new IllegalArgumentException("Entity value is missing some parts or containing more parts. " +
                                               "Expected: <entity-name>, given entity: " + parts);
        }
        entityParts.put(entityType, parts.get(index));
        break;
      case ARTIFACT:
      case APPLICATION:
        // artifact and application have version part to it. We don't support version for authorization purpose.
        // also throw exception only when the actual entity type in question is artifact/application not when we
        // reached here through recursive call on program
        if (parts.size() > 2 && index == (parts.size() - 1)) {
          throw new UnsupportedOperationException("Privilege can only be granted at the artifact/application level. " +
                                                    "If you are including version please remove it. Given entity: " +
                                                    parts);
        }
        if (parts.size() < 2 && index == (parts.size() - 1)) {
          throw new IllegalArgumentException("Entity value is missing some parts or containing more parts. " +
                                               "Expected: <namespace-name>.<entity-name>, given entity: " + parts);
        }
        checkParts(EntityType.NAMESPACE, parts, index - 1, entityParts);
        entityParts.put(entityType, parts.get(index));
        break;
      case DATASET:
      case DATASET_MODULE:
      case DATASET_TYPE:
      case SECUREKEY:
      case PROFILE:
        if (parts.size() != 2 && index == (parts.size() - 1)) {
          throw new IllegalArgumentException("Entity value is missing some parts or containing more parts. " +
                                               "Expected: <namespace-name>.<entity-name>, given entity: " + parts);
        }
        checkParts(EntityType.NAMESPACE, parts, index - 1, entityParts);
        entityParts.put(entityType, parts.get(index));
        break;
      case PROGRAMREFERENCE:
      case PROGRAM:
        // application have version part to it. We don't support version for authorization purpose.
        // also throw exception only when the actual entity type in question is program not when we
        // reached here through recursive call on some child of program (future security)
        if (parts.size() > 4 && index == (parts.size() - 1)) {
          throw new UnsupportedOperationException("Privilege can only be granted at the artifact/application level. " +
                                                    "If you are including version please remove it. Given entity: " +
                                                    parts);
        }
        if (parts.size() < 3 && index == (parts.size() - 1)) {
          throw new IllegalArgumentException("Entity value is missing some parts or containing more parts. " +
                                               "Expected: <namespace-name>.<app-name>.* or " +
                                               "<namespace-name>.<app-name>.<program-type>.<program-name>, " +
                                               "given entity: " + parts);
        }

        if (parts.size() == 3 && index == (parts.size() - 1)) {
          String program = parts.get(index);
          if (!"*".equals(program)) {
            throw new UnsupportedOperationException("When program type is not given, " +
                                                      "a program name can only contain a *");
          }
          checkParts(EntityType.APPLICATION, parts, index - 1, entityParts);
          entityParts.put(entityType, parts.get(index));
        } else {
          checkParts(EntityType.APPLICATION, parts, index - 2, entityParts);
          entityParts.put(entityType, parts.get(index - 1) + "." + parts.get(index));
        }
        break;
      case SYSTEM_APP_ENTITY:
        if (parts.size() != 4 && index == (parts.size() - 1)) {
          throw new IllegalArgumentException("Entity value is missing some parts or containing more parts. " +
                                               "Expected: <namespace-name>.<app-name>.* or " +
                                               "<namespace-name>.<app-name>.<entity-type>.<entity-name>, " +
                                               "given entity: " + parts);
        }
        checkParts(EntityType.NAMESPACE, parts, index - 3, entityParts);
        entityParts.put(entityType, parts.get(index - 2) + "." + parts.get(index - 1) + "." + parts.get(index));
        break;
        default:
        // although it should never happen
        throw new IllegalArgumentException(String.format("Entity type %s does not support authorization.", entityType));
    }
  }
}
