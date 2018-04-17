/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.etl.api.lineage.field;

/**
 * Interface for emitting field level operation.
 */
public interface FieldOperationEmitter {
  /**
   * Emits the specified field level operation. Multiple operations can be
   * emitted by a stage in the pipeline. However each operation emitted within the
   * single stage must have unique name.
   *
   * @param operation the operation to be emitted
   */
  void emit(Operation operation);
}
