/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package co.cask.cdap.proto;

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

  @SerializedName("Mapreduce")
  MAPREDUCE(3, Parameters.builder()
    .setCategoryName("mapreduce")
    .setPrettyName("MapReduce")
    .setListable(true)
    .setSchedulableType(SchedulableProgramType.MAPREDUCE)
    .setApiProgramType(co.cask.cdap.api.app.ProgramType.MAPREDUCE)
    .build()),

  @SerializedName("Workflow")
  WORKFLOW(4, Parameters.builder()
    .setCategoryName("workflows")
    .setPrettyName("Workflow")
    .setListable(true)
    .setDiscoverable(true)
    .setSchedulableType(SchedulableProgramType.WORKFLOW)
    .setApiProgramType(co.cask.cdap.api.app.ProgramType.WORKFLOW)
    .build()),

  @SerializedName("Service")
  SERVICE(6, Parameters.builder()
    .setCategoryName("services")
    .setPrettyName("Service")
    .setListable(true)
    .setDiscoverable(true)
    .setApiProgramType(co.cask.cdap.api.app.ProgramType.SERVICE)
    .build()),

  @SerializedName("Spark")
  SPARK(7, Parameters.builder()
    .setCategoryName("spark")
    .setPrettyName("Spark")
    .setListable(true)
    .setDiscoverable(true)
    .setSchedulableType(SchedulableProgramType.SPARK)
    .setApiProgramType(co.cask.cdap.api.app.ProgramType.SPARK)
    .build()),

  @SerializedName("Worker")
  WORKER(8, Parameters.builder()
    .setCategoryName("workers")
    .setPrettyName("Worker")
    .setListable(true)
    .setApiProgramType(co.cask.cdap.api.app.ProgramType.WORKER)
    .build()),

  CUSTOM_ACTION(9, Parameters.builder()
    .setCategoryName("custom")
    .setPrettyName("Custom")
    .setListable(false)
    .setSchedulableType(SchedulableProgramType.CUSTOM_ACTION)
    .build());

  private static final Map<String, ProgramType> CATEGORY_MAP;

  static {
    CATEGORY_MAP = new HashMap<>();
    for (ProgramType type : ProgramType.values()) {
      CATEGORY_MAP.put(type.getCategoryName(), type);
    }
  }

  private final int index;
  private final Parameters parameters;

  ProgramType(int type, Parameters parameters) {
    this.index = type;
    this.parameters = parameters;
  }

  public boolean isListable() {
    return parameters.isListable();
  }

  public boolean isDiscoverable() {
    return parameters.isDiscoverable();
  }

  public String getDiscoverablePrefix() {
    if (!isDiscoverable()) {
      throw new IllegalArgumentException("Program type " + name() + " is not discoverable");
    }
    return name().toLowerCase() + ".";
  }

  public String getCategoryName() {
    return parameters.getCategoryName();
  }

  public String getPrettyName() {
    return parameters.getPrettyName();
  }

  public String getScope() {
    return name().toLowerCase();
  }

  public co.cask.cdap.api.app.ProgramType getApiProgramType() {
    return parameters.getApiProgramType();
  }

  public SchedulableProgramType getSchedulableType() {
    if (parameters.getSchedulableType() == null) {
      throw new IllegalArgumentException(this + " is not a SchedulableProgramType");
    }
    return parameters.getSchedulableType();
  }

  public int getIndex() {
    return index;
  }

  public static ProgramType valueOfSchedulableType(SchedulableProgramType schedulableType) {
    for (ProgramType type : ProgramType.values()) {
      if (schedulableType.equals(type.parameters.getSchedulableType())) {
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
      throw new IllegalArgumentException(String.format("Unknown category name '%s'. Must be one of %s",
                                                       categoryName, String.join(",", CATEGORY_MAP.keySet())));
    }
    return type;
  }

  @Override
  public String toString() {
    return parameters.getPrettyName();
  }

  /**
   * Holds various properties of each ProgramType.
   */
  private static final class Parameters {
    private final String prettyName;
    private final boolean listable;
    private final String categoryName;
    private final SchedulableProgramType schedulableType;
    private final boolean discoverable;
    private co.cask.cdap.api.app.ProgramType apiProgramType;

    Parameters(String prettyName, Boolean listable, String categoryName, boolean discoverable,
               @Nullable SchedulableProgramType schedulableType,
               @Nullable co.cask.cdap.api.app.ProgramType apiProgramType) {
      if (prettyName == null) {
        throw new IllegalArgumentException("prettyName cannot be null");
      }
      if (listable == null) {
        throw new IllegalArgumentException("listable cannot be null");
      }
      if (categoryName == null) {
        throw new IllegalArgumentException("categoryName cannot be null");
      }
      this.prettyName = prettyName;
      this.listable = listable;
      this.categoryName = categoryName;
      this.discoverable = discoverable;
      this.schedulableType = schedulableType;
      this.apiProgramType = apiProgramType;
    }

    @Nullable
    SchedulableProgramType getSchedulableType() {
      return schedulableType;
    }

    String getPrettyName() {
      return prettyName;
    }

    boolean isListable() {
      return listable;
    }

    String getCategoryName() {
      return categoryName;
    }

    boolean isDiscoverable() {
      return discoverable;
    }

    @Nullable
    co.cask.cdap.api.app.ProgramType getApiProgramType() {
      return apiProgramType;
    }

    static Builder builder() {
      return new Builder();
    }

    /**
     * Builder for {@link ProgramType.Parameters}.
     */
    private static final class Builder {
      private String prettyName;
      private Boolean listable;
      private String categoryName;
      private boolean discoverable;
      private SchedulableProgramType schedulableType;
      private co.cask.cdap.api.app.ProgramType apiProgramType;

      Builder setSchedulableType(SchedulableProgramType schedulableType) {
        this.schedulableType = schedulableType;
        return this;
      }

      Builder setPrettyName(String prettyName) {
        this.prettyName = prettyName;
        return this;
      }

      Builder setListable(boolean listable) {
        this.listable = listable;
        return this;
      }

      Builder setCategoryName(String categoryName) {
        this.categoryName = categoryName;
        return this;
      }

      Builder setDiscoverable(boolean discoverable) {
        this.discoverable = discoverable;
        return this;
      }

      Builder setApiProgramType(co.cask.cdap.api.app.ProgramType apiProgramType) {
        this.apiProgramType = apiProgramType;
        return this;
      }

      Parameters build() {
        return new Parameters(prettyName, listable, categoryName, discoverable, schedulableType, apiProgramType);
      }
    }
  }
}
