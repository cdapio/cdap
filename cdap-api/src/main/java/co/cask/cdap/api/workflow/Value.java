/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.api.workflow;

import java.io.Serializable;

/**
 * Class representing the value of the key in the {@link WorkflowToken}.
 */
public class Value implements Serializable {

  private static final long serialVersionUID = -3420759818008526875L;

  private final String value;

  private Value(String value) {
    this.value = value;
  }

  /**
   * @return the boolean value
   */
  public boolean getAsBoolean() {
    return Boolean.parseBoolean(value);
  }

  /**
   * @return the int value
   */
  public int getAsInt() {
    return Integer.parseInt(value);
  }

  /**
   * @return the long value
   */
  public long getAsLong() {
    return Long.parseLong(value);
  }

  /**
   * @return the String value
   */
  @Override
  public String toString() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof Value)) {
      return false;
    }

    Value value1 = (Value) o;
    return value.equals(value1.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  /**
   * Return the {@link Value} representation of the input <code>boolean</code> parameter.
   * @param value the <code>boolean</code> value to be represented as {@link Value}
   * @return a {@link Value} representation of the argument
   */
  public static Value of(boolean value) {
    return new Value(String.valueOf(value));
  }

  /**
   * Return the {@link Value} representation of the input <code>int</code> parameter.
   * @param value the <code>int</code> value to be represented as {@link Value}
   * @return a {@link Value} representation of the argument
   */
  public static Value of(int value) {
    return new Value(String.valueOf(value));
  }

  /**
   * Return the {@link Value} representation of the input <code>long</code> parameter.
   * @param value the <code>long</code> value to be represented as {@link Value}
   * @return a {@link Value} representation of the argument
   */
  public static Value of(long value) {
    return new Value(String.valueOf(value));
  }

  /**
   * Return the {@link Value} representation of the input <code>String</code> parameter.
   * @param value the <code>String</code> value to be represented as {@link Value}
   * @return a {@link Value} representation of the argument
   */
  public static Value of(String value) {
    return new Value(value);
  }
}
