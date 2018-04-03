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
 */

package co.cask.cdap.api.lineage.field;

/**
 * Represents an input field of an operation. The field is uniquely
 * identified by its name and the name of the operation(origin) that
 * emits it as an output field.
 */
public class InputField {
  private final String origin;
  private final String name;

  private InputField(String origin, String name) {
    this.origin = origin;
    this.name = name;
  }

  /**
   * @return the name of the operation which created this input field
   */
  public String getOrigin() {
    return origin;
  }

  /**
   * @return the name of the input field
   */
  public String getName() {
    return name;
  }

  /**
   * Creates an instance of an input field.
   *
   * @param origin the name of the operation which created this input field
   * @param name the associated with the input field
   * @return the {@link InputField}
   */
  public static InputField of(String origin, String name) {
    return new InputField(origin, name);
  }
}
