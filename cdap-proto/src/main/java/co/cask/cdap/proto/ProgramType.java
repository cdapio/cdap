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

import com.google.gson.annotations.SerializedName;

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
  MAPREDUCE(3, "mapreduce", "Mapreduce", true),

  @SerializedName("Workflow")
  WORKFLOW(4, "workflows", "Workflow", true),

  @SerializedName("Webapp")
  WEBAPP(5, "webapp", "Webapp", false),

  @SerializedName("Service")
  SERVICE(6, "services", "Service", false),

  @SerializedName("Spark")
  SPARK(7, "spark", "Spark", true);

  private final int programType;
  private final String prettyName;
  private final boolean listable;
  private final String categoryName;

  private ProgramType(int type, String categoryName, String prettyName, boolean listable) {
    this.programType = type;
    this.categoryName = categoryName;
    this.prettyName = prettyName;
    this.listable = listable;
  }

  public boolean isListable() {
    return listable;
  }

  public String getCategoryName() {
    return categoryName;
  }

  public String getPrettyName() {
    return prettyName;
  }

  public static ProgramType valueOfPrettyName(String pretty) {
    return valueOf(pretty.toUpperCase());
  }

  @Override
  public String toString() {
    return prettyName;
  }

}
