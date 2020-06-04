/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.join.error;

import java.util.Objects;

/**
 * An error related to a specific join key field.
 */
public class JoinKeyFieldError extends JoinError {
  private final String stageName;
  private final String keyField;

  public JoinKeyFieldError(String stageName, String keyField, String message) {
    super(Type.JOIN_KEY_FIELD, message, null);
    this.stageName = stageName;
    this.keyField = keyField;
  }

  public String getStageName() {
    return stageName;
  }

  public String getKeyField() {
    return keyField;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    JoinKeyFieldError that = (JoinKeyFieldError) o;
    return Objects.equals(stageName, that.stageName) &&
      Objects.equals(keyField, that.keyField);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), stageName, keyField);
  }
}
