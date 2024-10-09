/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.exception;

/**
 * Enum representing the different phases of a stage where error can occur.
 */
public enum ErrorPhase {
  SPLITTING("Splitting"),
  READING("Reading"),
  VALIDATING_OUTPUT_SPECS("Validating Output Specs"),
  WRITING("Writing"),
  COMMITTING("Committing");

  private final String displayName;

  ErrorPhase(String displayName) {
    this.displayName = displayName;
  }

  /**
   * Returns a string representation of the error phase enum.
   */
  @Override
  public String toString() {
    return displayName;
  }
}
