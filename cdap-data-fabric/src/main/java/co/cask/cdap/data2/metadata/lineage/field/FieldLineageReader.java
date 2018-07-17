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

package co.cask.cdap.data2.metadata.lineage.field;

import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.api.lineage.field.WriteOperation;
import co.cask.cdap.proto.metadata.lineage.ProgramRunOperations;

import java.util.Set;

/**
 * Interface for reading the {@link FieldLineageDataset} store.
 */
public interface FieldLineageReader {
  /**
   * Get the set of fields written to the EndPoint by field lineage {@link WriteOperation},
   * over the given time range.
   *
   * @param endPoint the EndPoint for which the fields need to be returned
   * @param start start time (inclusive) in milliseconds
   * @param end end time (exclusive) in milliseconds
   * @return set of fields written to a given EndPoint
   */
  Set<String> getFields(EndPoint endPoint, long start, long end);

  /**
   * Get the incoming summary for the specified EndPointField over a given time range.
   * Incoming summary consists of set of EndPointFields which participated in the computation
   * of the given EndPointField.
   *
   * @param endPointField the EndPointField for which incoming summary to be returned
   * @param start start time (inclusive) in milliseconds
   * @param end end time (exclusive) in milliseconds
   * @return the set of EndPointFields
   */
  Set<EndPointField> getIncomingSummary(EndPointField endPointField, long start, long end);

  /**
   * Get the outgoing summary for the specified EndPointField in a given time range.
   * Outgoing summary consists of set of EndPointFields which were computed from the
   * specified EndPointField.
   *
   * @param endPointField the EndPointField for which outgoing summary to be returned
   * @param start start time (inclusive) in milliseconds
   * @param end end time (exclusive) in milliseconds
   * @return the set of EndPointFields
   */
  Set<EndPointField> getOutgoingSummary(EndPointField endPointField, long start, long end);

  /**
   * Get the set of operations which were responsible for computing the given field
   * of the specified EndPoint over a given time range. Along with the operations, program
   * runs are also returned which performed these operations.
   *
   * @param endPointField the EndPointField for which incoming operations to be returned
   * @param start start time (inclusive) in milliseconds
   * @param end end time (exclusive) in milliseconds
   * @return the operations and program run information
   */
  Set<ProgramRunOperations> getIncomingOperations(EndPointField endPointField, long start, long end);

  /**
   * Get the set of operations which were performed on the field of the specified EndPoint
   * to compute the fields of the downstream EndPoints. Along with the operations, program
   * runs are also returned which performed these operations.
   *
   * @param endPointField the EndPointField for which outgoing operations to be returned
   * @param start start time (inclusive) in milliseconds
   * @param end end time (exclusive) in milliseconds
   * @return the operations and program run information
   */
  Set<ProgramRunOperations> getOutgoingOperations(EndPointField endPointField, long start, long end);
}
