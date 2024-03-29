/*
 * Copyright © 2014 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2;

/**
 * Thrown when operation conflicts with existing data set types in the system. NOTE: for now we
 * don't want to leak this exception class into dev-facing APIs, see {@link
 * io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry}.
 */
public class TypeConflictException extends IllegalArgumentException {

  public TypeConflictException(String message) {
    super(message);
  }

  public TypeConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}
