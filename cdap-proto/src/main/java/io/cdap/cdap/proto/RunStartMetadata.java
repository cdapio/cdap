package io.cdap.cdap.proto;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

public class RunStartMetadata {

  public enum Type {
    TIME("time"),
    PARTITION("partition"),
    PROGRAM_STATUS("program-status"),
    AND("and"),
    OR("or"),
    MANUAL("manual");

    private static final Map<String, Type> CATEGORY_MAP;

    static {
      CATEGORY_MAP = new HashMap<>();
      for (Type type : Type.values()) {
        CATEGORY_MAP.put(type.getCategoryName(), type);
      }
    }

    private final String categoryName;

    Type(String categoryName) {
      this.categoryName = categoryName;
    }

    /**
     * @return The category name of the type.
     */
    public String getCategoryName() {
      return categoryName;
    }

    /**
     * Get the corresponding type with the given category name of the type
     *
     * @param categoryName the category name to get the type for
     * @return the corresponding type of the given category name
     */
    public static Type valueOfCategoryName(String categoryName) {
      Type type = CATEGORY_MAP.get(categoryName);
      if (type == null) {
        throw new IllegalArgumentException(String.format("Unknown category name '%s'. Must be one of %s",
                                                         categoryName, String.join(",", CATEGORY_MAP.keySet())));
      }
      return type;
    }
  }

  private final Type type;
  @Nullable
  private final TriggeringInfo triggeringInfo;

  public RunStartMetadata(Type type, @Nullable TriggeringInfo triggeringInfo) {
    this.type = type;
    this.triggeringInfo = triggeringInfo;
  }

  public Type getType() {
    return type;
  }

  @Nullable
  public TriggeringInfo getTriggeringInfo() {
    return triggeringInfo;
  }
}
