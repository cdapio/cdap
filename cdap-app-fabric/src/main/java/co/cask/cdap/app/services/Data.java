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

package co.cask.cdap.app.services;

/**
 * Defines types of data supported by the system.
 */
public enum Data {
  STREAM(1, "Stream"),
  DATASET(2, "Dataset");

  private final int dataType;
  private final String name;

  Data(int type, String prettyName) {
    this.dataType = type;
    this.name = prettyName;
  }

  public String prettyName() {
    return name;
  }

  public static Data valueofPrettyName(String pretty) {
    return valueOf(pretty.toUpperCase());
  }
}
