/*
 * Copyright © 2014 Cask Data, Inc.
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
import com.google.common.base.Preconditions;
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
  FLOW(1, Parameters.builder()
    .setCategoryName("flows")
    .setPrettyName("Flow")
    .setListable(false)
    .build()),

  @SerializedName("Procedure")
  PROCEDURE(2, Parameters.builder()
    .setCategoryName("procedures")
    .setPrettyName("Procedure")
    .setListable(true)
    .build()),

  @SerializedName("Mapreduce")
  MAPREDUCE(3, Parameters.builder()
    .setCategoryName("mapreduce")
    .setPrettyName("MapReduce")
    .setListable(true)
    .setSchedulableType(SchedulableProgramType.MAPREDUCE)
    .build()),

  @SerializedName("Workflow")
  WORKFLOW(4, Parameters.builder()
    .setCategoryName("workflows")
    .setPrettyName("Workflow")
    .setListable(true)
    .setSchedulableType(SchedulableProgramType.WORKFLOW)
    .build()),

  @SerializedName("Webapp")
  WEBAPP(5, Parameters.builder()
    .setCategoryName("webapp")
    .setPrettyName("Webapp")
    .setListable(false)
    .build()),

  @SerializedName("Service")
  SERVICE(6, Parameters.builder()
    .setCategoryName("services")
    .setPrettyName("Service")
    .setListable(true)
    .build()),

  @SerializedName("Spark")
  SPARK(7, Parameters.builder()
    .setCategoryName("spark")
    .setPrettyName("Spark")
    .setListable(true)
    .setSchedulableType(SchedulableProgramType.SPARK)
    .build()),

  @SerializedName("Worker")
  WORKER(8, Parameters.builder()
    .setCategoryName("workers")
    .setPrettyName("Worker")
    .setListable(true)
    .build());

  private static final Map<String, ProgramType> CATEGORY_MAP;

  static {
    CATEGORY_MAP = new HashMap<String, ProgramType>();
    for (ProgramType type : ProgramType.values()) {
      CATEGORY_MAP.put(type.getCategoryName(), type);
    }
  }

  private final int index;
  private final Parameters parameters;

  private ProgramType(int type, Parameters parameters) {
    this.index = type;
    this.parameters = parameters;
  }

  public boolean isListable() {
    return parameters.listable;
  }

  public String getCategoryName() {
    return parameters.getCategoryName();
  }

  public String getPrettyName() {
    return parameters.getPrettyName();
  }

  public SchedulableProgramType getSchedulableType() {
    return parameters.getSchedulableType();
  }

  public int getIndex() {
    return index;
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

    public Parameters(String prettyName, Boolean listable, String categoryName,
                      @Nullable SchedulableProgramType schedulableType) {
      Preconditions.checkArgument(prettyName != null, "prettyName cannot be null");
      Preconditions.checkArgument(listable != null, "listable cannot be null");
      Preconditions.checkArgument(categoryName != null, "categoryName cannot be null");
      this.prettyName = prettyName;
      this.listable = listable;
      this.categoryName = categoryName;
      this.schedulableType = schedulableType;
    }

    public SchedulableProgramType getSchedulableType() {
      return schedulableType;
    }

    public String getPrettyName() {
      return prettyName;
    }

    public boolean isListable() {
      return listable;
    }

    public String getCategoryName() {
      return categoryName;
    }

    public static Builder builder() {
      return new Builder();
    }

    /**
     * Builder for {@link ProgramType.Parameters}.
     */
    private static final class Builder {
      private String prettyName;
      private Boolean listable;
      private String categoryName;
      private SchedulableProgramType schedulableType;

      public Builder setSchedulableType(SchedulableProgramType schedulableType) {
        this.schedulableType = schedulableType;
        return this;
      }

      public Builder setPrettyName(String prettyName) {
        this.prettyName = prettyName;
        return this;
      }

      public Builder setListable(boolean listable) {
        this.listable = listable;
        return this;
      }

      public Builder setCategoryName(String categoryName) {
        this.categoryName = categoryName;
        return this;
      }

      public Parameters build() {
        return new Parameters(prettyName, listable, categoryName, schedulableType);
      }
    }
  }

}
