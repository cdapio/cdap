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

package co.cask.cdap.explore.service;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryHandle;
import com.google.common.util.concurrent.Service;

import java.sql.SQLException;

/**
 * Interface for service exploring datasets.
 */
public interface ExploreService extends Service, Explore {

  /**
   * Execute a sequence of Hive SQL statements. All but the last statement are executed synchronously, and
   * the last statement is run asynchronously. The returned {@link QueryHandle} can be used to get the
   * status/result of the operation.
   *
   * @param namespace namespace to run the query in.
   * @param statements SQL statement.
   * @return {@link QueryHandle} representing the operation.
   * @throws ExploreException on any error executing statement.
   * @throws SQLException if there are errors in the SQL statement.
   */
  QueryHandle execute(Id.Namespace namespace, String[] statements) throws ExploreException, SQLException;

}
