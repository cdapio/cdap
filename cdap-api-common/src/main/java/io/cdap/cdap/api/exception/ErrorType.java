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

package io.cdap.cdap.api.exception;

/**
 * Enum representing different types of errors due to which a program can fail.
 * <p>
 * This enum classifies errors into three broad categories:
 * </p>
 * <ul>
 *   <li><b>SYSTEM</b> - Represents system-level errors that are typically caused by infrastructure,
 *   network, or external dependencies, rather than user actions.</li>
 *   <li><b>USER</b> - Represents errors caused by user input or actions that violate expected rules
 *   or constraints.</li>
 *   <li><b>UNKNOWN</b> - Represents errors of an indeterminate type, where the cause cannot be
 *   classified as either system or user error.</li>
 * </ul>
 */
public enum ErrorType {
  UNKNOWN, // Default type for safety during deserialization
  SYSTEM,
  USER,
}
