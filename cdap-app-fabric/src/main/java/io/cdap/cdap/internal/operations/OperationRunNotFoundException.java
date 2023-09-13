/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.operations;

import io.cdap.cdap.common.NotFoundException;

/**
 * Exception thrown when an operation run with the specified id not found in the specified namespace
 */
public class OperationRunNotFoundException extends NotFoundException {
  public OperationRunNotFoundException(String namespace, String runId) {
    super(String.format("Operation run %s does not exist in namespace %s", runId, namespace));
  }
}
