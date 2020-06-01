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

import io.cdap.cdap.etl.api.join.JoinField;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * An error with one of the selected output fields.
 */
public class SelectedFieldError extends JoinError {
  private final JoinField field;

  public SelectedFieldError(JoinField field, String message) {
    this(field, message, null);
  }

  public SelectedFieldError(JoinField field, String message, @Nullable String correctiveAction) {
    super(JoinError.Type.SELECTED_FIELD, message, correctiveAction);
    this.field = field;
  }

  /**
   * @return the invalid selected field
   */
  public JoinField getField() {
    return field;
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
    SelectedFieldError that = (SelectedFieldError) o;
    return Objects.equals(field, that.field);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), field);
  }
}
