/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.api.plugin;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * An invalid plugin property.
 */
public class InvalidPluginProperty {
  private final String name;
  private final String errorMessage;
  private final Exception cause;

  public InvalidPluginProperty(String name, String errorMessage) {
    this(name, errorMessage, null);
  }

  public InvalidPluginProperty(String name, Exception cause) {
    this(name, cause.getMessage(), cause);
  }

  private InvalidPluginProperty(String name, String errorMessage, Exception cause) {
    this.name = name;
    this.errorMessage = errorMessage;
    this.cause = cause;
  }

  public String getName() {
    return name;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  @Nullable
  public Exception getCause() {
    return cause;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    InvalidPluginProperty that = (InvalidPluginProperty) o;

    return Objects.equals(name, that.name) && Objects.equals(errorMessage, that.errorMessage) &&
      Objects.equals(cause, that.cause);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, errorMessage, errorMessage);
  }
}
