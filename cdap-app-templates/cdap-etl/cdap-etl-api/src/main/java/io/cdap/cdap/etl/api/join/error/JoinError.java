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
import javax.annotation.Nullable;

/**
 * An error that contributed to an invalid JoinDefinition.
 */
public class JoinError {
  private final Type type;
  private final String message;
  private final String correctiveAction;

  public JoinError(String message) {
    this(message, null);
  }

  public JoinError(String message, @Nullable String correctiveAction) {
    this(Type.GENERAL, message, correctiveAction);
  }

  protected JoinError(Type type, String message, @Nullable String correctiveAction) {
    this.type = type;
    this.message = message;
    this.correctiveAction = correctiveAction == null ? "" : correctiveAction;
  }

  public Type getType() {
    return type;
  }

  public String getMessage() {
    return message;
  }

  public String getCorrectiveAction() {
    return correctiveAction;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JoinError joinError = (JoinError) o;
    return type == joinError.type &&
      Objects.equals(message, joinError.message) &&
      Objects.equals(correctiveAction, joinError.correctiveAction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, message, correctiveAction);
  }

  /**
   * Type of join error
   */
  public enum Type {
    GENERAL,
    SELECTED_FIELD,
    JOIN_KEY,
    JOIN_KEY_FIELD,
    OUTPUT_SCHEMA,
    DISTRIBUTION_SIZE,
    DISTRIBUTION_STAGE,
    BROADCAST
  }
}
