package co.cask.cdap.proto.security;


import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.EntityId;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static co.cask.cdap.proto.id.EntityId.IDSTRING_TYPE_SEPARATOR;

/**
 * Class which represents entities on which privileges can be granted/revoked.
 */
public class Authorizable {
  EntityType entityType;
  List<String> entityParts;

  public Authorizable(String entityString) {
    if (entityString == null || entityString.isEmpty()) {
      throw new IllegalArgumentException("A valid authorizable must be provided.");
    }
    String[] typeAndId = entityString.split(IDSTRING_TYPE_SEPARATOR, 2);
    if (typeAndId.length != 2) {
      throw new IllegalArgumentException(
        String.format("Expected type separator '%s' to be in the ID string: %s", IDSTRING_TYPE_SEPARATOR, entityString));
    }

    String typeString = typeAndId[0];
    EntityType type = EntityType.valueOf(typeString.toUpperCase());
    String idString = typeAndId[1];
    List<String> idParts = Arrays.asList(EntityId.IDSTRING_PART_SEPARATOR_PATTERN.split(idString));
    if (!checkParts(type, idParts)) {
      throw new IllegalArgumentException("");
    }
    entityType = type;
    entityParts = idParts;
  }

  private boolean checkParts(EntityType entityType, List<String> parts) {
    switch (entityType) {
      case INSTANCE:
      case NAMESPACE:
        return parts.size() == 1;
      case ARTIFACT:
      case APPLICATION:
        // artifact and application have version part to it. We don't support version for authorization purpose.
        if (parts.size() == 3) {
          throw new UnsupportedOperationException("Privilege can only be granted at the artifact level. If you are " +
                                                    "including a artifact version please remove it.");
        }
        return parts.size() == 2;
      case DATASET:
      case DATASET_MODULE:
      case DATASET_TYPE:
      case STREAM:
      case SECUREKEY:
        return parts.size() == 2;
      case PROGRAM:
        return parts.size() == 4;
      default:
        // although it should never happen
        throw new IllegalArgumentException(String.format("Entity type %s does not support authorization.", entityType));
    }
  }

  public EntityType getEntityType() {
    return entityType;
  }

  public List<String> getEntityParts() {
    return entityParts;
  }

  @Override
  public String toString() {
    return "Authorizable{" +
      "entityType=" + entityType +
      ", entityParts=" + entityParts +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Authorizable that = (Authorizable) o;
    return entityType == that.entityType && entityParts.equals(that.entityParts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entityType, entityParts);
  }
}
