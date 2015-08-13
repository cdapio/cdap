/*
 * Copyright © 2015 Cask Data, Inc.
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

/**
 * <p>
 * This package contains the DataCleansing Application that filters records that do not match a given schema.
 *
 * This DataCleansing Application consists of these programs and datasets:
 * </p>
 *
 * <ol>
 *   <li>
 *   {@link co.cask.cdap.examples.datacleansing.DataCleansingService} that allows writing to the
 *   rawRecords PartitionedFileSets.
 *   </li>
 *
 *   <li>
 *   A MapReduce named {@link co.cask.cdap.examples.datacleansing.DataCleansingMapReduce} that reads the
 *   files from a PartitionedFileSet, applies a filter to remove "unclean" records, based upon a particular
 *   schema, and outputs the records to an output PartitionedFileSet. Each time the job runs, it processes
 *   only the files of the newly created partitions.
 *   </li>
 *
 *   <li>
 *   Three Datasets used by the MapReduce and Service:
 *     <ul>
 *       <li>A PartitionedFileSet named rawRecords which serves as the input data for DataCleansingMapReduce.</li>
 *       <li>A PartitionedFileSet named cleanRecords which serves as output for DataCleansingMapReduce.</li>
 *       <li>A KeyValueTable named consumingState which keeps track of the state of the DataCleansingMapReduce
 *       so that each time it is run, it only processes files of newly created Partitions.</li>
 *     </ul>
 *   </li>
 * </ol>
 */
package co.cask.cdap.examples.datacleansing;
