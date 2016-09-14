/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.lineage;

import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.base.Predicate;

import java.util.Set;

/**
 * This interface defines method to read from lineage store.
 * It is needed to break circular dependency.
 */
public interface LineageStoreReader {

  /**
   * @return a set of entities (program and data it accesses) associated with a program run.
   */
  Set<NamespacedEntityId> getEntitiesForRun(ProgramRunId run);

  /**
   * Fetch program-dataset access information for a dataset for a given period.
   *
   * @param datasetInstance dataset for which to fetch access information
   * @param start start time period
   * @param end end time period
   * @param filter filter to be applied on result set
   * @return program-dataset access information
   */
  Set<Relation> getRelations(DatasetId datasetInstance, long start, long end,
                             Predicate<Relation> filter);

  /**
   * Fetch program-stream access information for a dataset for a given period.
   *
   * @param stream stream for which to fetch access information
   * @param start start time period
   * @param end end time period
   * @param filter filter to be applied on result set
   * @return program-stream access information
   */
  Set<Relation> getRelations(StreamId stream, long start, long end,
                             Predicate<Relation> filter);

  /**
   * Fetch program-dataset access information for a program for a given period.
   *
   * @param program program for which to fetch access information
   * @param start start time period
   * @param end end time period
   * @param filter filter to be applied on result set
   * @return program-dataset access information
   */
  Set<Relation> getRelations(ProgramId program, long start, long end,
                             Predicate<Relation> filter);
}
