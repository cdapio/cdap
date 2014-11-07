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

package co.cask.cdap.explore.client;

import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryResult;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;

/**
 * Results of an Explore statement execution.
 */
public interface ExploreExecutionResult extends Iterator<QueryResult>, Closeable {

  /**
   * @return the current fetch size for this object
   */
  int getFetchSize();

  /**
   * Gives this object a hint as to the number of rows that should be fetched from the server when more rows are needed
   * for this object. If the fetch size specified is zero, this object ignores the value and is free to make its own
   * best guess as to what the fetch size should be. The fetch size may be changed at any time.
   *
   * @param fetchSize the number of rows to fetch
   */
  void setFetchSize(int fetchSize);

  /**
   * Fetch the schema of this execution result. The schema of a query is only available when the query is finished.
   *
   * @return list of {@link ColumnDesc} representing the schema of the results. Empty list if there are no results.
   * @throws ExploreException on any error fetching schema.
   */
  List<ColumnDesc> getResultSchema() throws ExploreException;

  /**
   * Some SQL statememt never return results - like CREATE statements - for which this method will return false.
   * SQL queries should always return true.
   *
   * @return true if this {@link ExploreExecutionResult} may contain results.
   */
  boolean canContainResults();
}
