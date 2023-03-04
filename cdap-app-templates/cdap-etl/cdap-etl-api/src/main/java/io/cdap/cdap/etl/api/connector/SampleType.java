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

package io.cdap.cdap.etl.api.connector;

import com.google.gson.annotations.SerializedName;

/**
 * Enum representing type of sampling method
 */
public enum SampleType {

  /**
   * Sample first [x] rows
   */
  @SerializedName("first")
  FIRST("first"),

  /**
   * Sample random [x] rows
   */
  @SerializedName("random")
  RANDOM("random"),

  /**
   * Sample [x] rows proportionally based on specified strata column
   */
  @SerializedName("stratified")
  STRATIFIED("stratified"),

  /**
   * Use default sampling method
   */
  DEFAULT("default");

  private final String name;

  SampleType(String name) {
    this.name = name;
  }

  public static SampleType fromString(String name) {
    for (SampleType type : SampleType.values()) {
      if (type.name.equalsIgnoreCase(name)) {
        return type;
      }
    }
    return DEFAULT;
  }
}
