/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.api;

import co.cask.cdap.api.schedule.SchedulableProgramType;
import com.google.gson.annotations.SerializedName;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Defines types of programs supported by the system.
 */
public enum ProgramType {

  // @SerializedName to maintain backwards-compatibility

  @SerializedName("Flow")
  FLOW(1, "flows", "Flow", true),

  @SerializedName("Procedure")
  PROCEDURE(2, "procedures", "Procedure", true),

  @SerializedName("Mapreduce")
  MAPREDUCE(3, "mapreduce", "MapReduce", true, SchedulableProgramType.MAPREDUCE),

  @SerializedName("Workflow")
  WORKFLOW(4, "workflows", "Workflow", true, SchedulableProgramType.WORKFLOW),

  @SerializedName("Webapp")
  WEBAPP(5, "webapp", "Webapp", false),

  @SerializedName("Service")
  SERVICE(6, "services", "Service", true),

  @SerializedName("Spark")
  SPARK(7, "spark", "Spark", true, SchedulableProgramType.SPARK),

  @SerializedName("Worker")
  WORKER(8, "workers", "Worker", true),

  CUSTOM_ACTION(9, "custom", "Custom", false, SchedulableProgramType.CUSTOM_ACTION);

  private static final Map<String, ProgramType> CATEGORY_MAP;

  static {
    CATEGORY_MAP = new HashMap<String, ProgramType>();
    for (ProgramType type : ProgramType.values()) {
      CATEGORY_MAP.put(type.getCategoryName(), type);
    }
  }

  private final int index;
  private final String categoryName;
  private final String prettyName;
  private final boolean isListable;
  private final SchedulableProgramType schedulableProgramType;

  private ProgramType(int index, String categoryName, String prettyName, boolean isListable,
                      @Nullable SchedulableProgramType schedulableProgramType) {
    this.index = index;
    this.categoryName = categoryName;
    this.prettyName = prettyName;
    this.isListable = isListable;
    this.schedulableProgramType = schedulableProgramType;
  }

  private ProgramType(int index, String categoryName, String prettyName, boolean isListable) {
    this(index, categoryName, prettyName, isListable, null);
  }

  public boolean isListable() {
    return isListable;
  }

  public String getCategoryName() {
    return categoryName;
  }

  public String getPrettyName() {
    return prettyName;
  }

  @Nullable
  public SchedulableProgramType getSchedulableType() {
    return schedulableProgramType;
  }

  public int getIndex() {
    return index;
  }

  public static ProgramType valueOfSchedulableType(SchedulableProgramType schedulableType) {
    for (ProgramType type : ProgramType.values()) {
      if (schedulableType.equals(type.getSchedulableType())) {
        return type;
      }
    }
    throw new IllegalArgumentException("No ProgramType found for SchedulableProgramType " + schedulableType);
  }

  public static ProgramType valueOfPrettyName(String pretty) {
    return valueOf(pretty.toUpperCase());
  }

  public static ProgramType valueOfCategoryName(String categoryName) {
    ProgramType type = CATEGORY_MAP.get(categoryName);
    if (type == null) {
      throw new IllegalArgumentException("Unknown category name " + categoryName);
    }
    return type;
  }

  @Override
  public String toString() {
    return getPrettyName();
  }
}
