/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.proto;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This class contains the information of how a run is triggered
 */
public class RunStartMetadata {

  /**
   * Defines how the run has been started
   */
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
