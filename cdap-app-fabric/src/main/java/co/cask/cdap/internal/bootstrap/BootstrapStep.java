/*
 * Copyright © 2018 Cask Data, Inc.
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
 *
 */

package co.cask.cdap.internal.bootstrap;

import com.google.gson.JsonObject;

/**
 * Definition for a single step in a bootstrap process. Defines what operation to perform, whether it should be executed
 * every time CDAP starts up or just once, and any arguments required to perform the operation.
 */
public class BootstrapStep {
  private final String label;
  private final Type type;
  private final RunCondition runCondition;
  private final JsonObject arguments;

  public BootstrapStep(String label, Type type, RunCondition runCondition, JsonObject arguments) {
    this.label = label;
    this.type = type;
    this.runCondition = runCondition;
    this.arguments = arguments;
  }

  public String getLabel() {
    return label;
  }

  public Type getType() {
    return type;
  }

  public RunCondition getRunCondition() {
    // can be null when deserialized through gson
    return runCondition == null ? RunCondition.ONCE : runCondition;
  }

  public JsonObject getArguments() {
    // can be null when deserialized through gson
    return arguments == null ? new JsonObject() : arguments;
  }

  /**
   * Validate that all required fields exist.
   *
   * @throws IllegalArgumentException if the step is invalid
   */
  public void validate() {
    if (label == null || label.isEmpty()) {
      throw new IllegalArgumentException("A bootstrap step must contain a label.");
    }
    if (type == null) {
      throw new IllegalArgumentException("A bootstrap step must contain a type.");
    }
  }

  /**
   * Bootstrap step type
   */
  public enum Type {
    LOAD_SYSTEM_ARTIFACTS,
    CREATE_DEFAULT_NAMESPACE,
    CREATE_NATIVE_PROFILE,
    CREATE_SYSTEM_PROFILE,
    SET_SYSTEM_PROPERTIES,
    CREATE_APPLICATION,
    START_PROGRAM
  }

  /**
   * When a step should be run
   */
  public enum RunCondition {
    ONCE,
    ALWAYS
  }
}
