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
 *
 */

package io.cdap.cdap.etl.api.validation;

/**
 * Indicates a config property is invalid.
 *
 * Deprecated since 6.1.0. Use {@link ValidationFailure} instead.
 */
@Deprecated
public class InvalidConfigPropertyException extends InvalidStageException {
  private final String property;

  public InvalidConfigPropertyException(String message, String property) {
    super(message);
    this.property = property;
  }

  public InvalidConfigPropertyException(String message, Throwable cause, String property) {
    super(message, cause);
    this.property = property;
  }

  public String getProperty() {
    return property;
  }
}
