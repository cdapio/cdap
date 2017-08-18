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
package co.cask.cdap.proto.security;


import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.EntityId;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Class which represents entities on which privileges can be granted/revoked.
 */
public class Authorizable {
  private final EntityType entityType;
  private Map<EntityType, String> entityParts = new LinkedHashMap<>();

  public Authorizable(String entityString) {
    if (entityString == null || entityString.isEmpty()) {
      throw new IllegalArgumentException("A valid authorizable must be provided.");
    }
    String[] typeAndId = entityString.split(EntityId.IDSTRING_TYPE_SEPARATOR, 2);
    if (typeAndId.length != 2) {
      throw new IllegalArgumentException(
        String.format("Expected type separator '%s' to be in the ID string: %s", EntityId.IDSTRING_TYPE_SEPARATOR,
                      entityString));
    }

    String typeString = typeAndId[0];
    EntityType type = EntityType.valueOf(typeString.toUpperCase());
    String idString = typeAndId[1];
    List<String> idParts = Arrays.asList(EntityId.IDSTRING_PART_SEPARATOR_PATTERN.split(idString));
    checkParts(type, idParts, idParts.size() - 1);
    entityType = type;
  }

  public static Authorizable fromEntityId(EntityId entityId) {
    if (entityId == null) {
      throw new IllegalArgumentException("EntityId is required.");
    }
    String entity = entityId.toString();
    // drop the version for artifact or application
    if (entityId.getEntityType().equals(EntityType.ARTIFACT) ||
      entityId.getEntityType().equals(EntityType.APPLICATION)) {
      int endIndex = entity.indexOf(EntityId.IDSTRING_PART_SEPARATOR,
                                    entity.indexOf(EntityId.IDSTRING_PART_SEPARATOR) + 1);
      entity = entity.substring(0, endIndex); // not forgot to put check if(endIndex != -1)
    }
    return new Authorizable(entity);
  }


  private void checkParts(EntityType entityType, List<String> parts, int curType) {
    switch (entityType) {
      case INSTANCE:
      case NAMESPACE:
        entityParts.put(entityType, parts.get(curType));
        break;
      case ARTIFACT:
      case APPLICATION:
        // artifact and application have version part to it. We don't support version for authorization purpose.
        if (parts.size() == 3) {
          throw new UnsupportedOperationException("Privilege can only be granted at the artifact/application level. " +
                                                    "If you are including version please remove it. Given entity: " +
                                                    parts);
        }
        checkParts(EntityType.NAMESPACE, parts, curType - 1);
        entityParts.put(entityType, parts.get(curType));
        break;
      case DATASET:
      case DATASET_MODULE:
      case DATASET_TYPE:
      case STREAM:
      case SECUREKEY:
        checkParts(EntityType.NAMESPACE, parts, curType - 1);
        entityParts.put(entityType, parts.get(curType));
        break;
      case PROGRAM:
        checkParts(EntityType.APPLICATION, parts, curType - 2);
        entityParts.put(entityType, parts.get(curType - 1) + "." + parts.get(curType));
        break;
      default:
        // although it should never happen
        throw new IllegalArgumentException(String.format("Entity type %s does not support authorization.", entityType));
    }
  }

  public EntityType getEntityType() {
    return entityType;
  }

  public Map<EntityType, String> getEntityParts() {
    return Collections.unmodifiableMap(entityParts);
  }

  @Override
  public String toString() {
    // the to string is done in this way to matain compatibility with EntityId.toString()
    StringBuilder result = new StringBuilder();
    result.append(entityType.name().toLowerCase());

    String separator = EntityId.IDSTRING_TYPE_SEPARATOR;
    for (String part : entityParts.values()) {
      result.append(separator).append(part);
      separator = EntityId.IDSTRING_PART_SEPARATOR;
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
    return entityType == that.entityType && entityParts.equals(that.entityParts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entityType, entityParts);
  }
}
