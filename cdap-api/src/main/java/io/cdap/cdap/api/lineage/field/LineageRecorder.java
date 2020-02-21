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

package io.cdap.cdap.api.lineage.field;

import java.util.Collection;

/**
 * Interface for recording lineage information.
 */
public interface LineageRecorder {
  /**
   * Record specified operations for the lineage purpose. This method can be called multiple
   * times by a program. Operations provided during each call will be accumulated in a collection
   * for the lineage. This collection of all operations provided across multiple calls must have
   * unique names. For the completeness, this collection should have at least one operation of
   * type {@link ReadOperation} and one operation of type {@link WriteOperation}.
   *
   * @param operations the operations to be recorded
   */
  void record(Collection<? extends Operation> operations);

  /**
   * Flush the existing record, this method will flush all the existing operations to the writer, and clear
   * the recorded operations. The existing operations should be complete, they should have at least one
   * operation of type {@link ReadOperation} and one operation of type {@link WriteOperation}. If the operations are
   * not complete, {@link IllegalArgumentException} will be thrown and existing records will be preserved.
   *
   * For batch programs(workflow, batch spark, mapreduce), the method is called automatically at the end of the
   * successful run.
   * For realtime programs(worker, service, spark streaming), the method has to be manually called to write the
   * lineage since realtime program will not stop automatically.
   *
   * @throws IllegalArgumentException if the validation of the field operations fails, this can happen when there is
   * no read or write operations, operations have same names, or there is a cycle in the operations.
   */
  void flushLineage() throws IllegalArgumentException;
}
