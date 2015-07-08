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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class representing the value of the key in the {@link WorkflowToken}.
 */
public class Value {
  private final String value;
  private static final Logger LOG = LoggerFactory.getLogger(Value.class);

  public Value(String value) {
    this.value = value;
  }

  /**
   * @return the value as String
   */
  public String getAsString() {
    return value;
  }

  /**
   * @return the Long value, or null if cannot be converted
   */
  public Long getAsLong() {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      LOG.warn(String.format("Cannot convert value '%s' to Long.", value));
      return null;
    }
  }

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
}
