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

package co.cask.cdap.test;

import java.io.IOException;
import java.util.Map;

/**
 * Class for sending queries to running {@link co.cask.cdap.api.procedure.Procedure Procedure}.
 */
public interface ProcedureClient {

  /**
   * Makes a query to the procedure.
   *
   * @param method Name of the procedure method to query against.
   * @param arguments Arguments to be submitted to the procedure for the query.
   * @return A byte array representing the response.
   * @throws java.io.IOException When there is error querying the procedure.
   */
  byte[] queryRaw(String method, Map<String, String> arguments) throws IOException;


  /**
   * Makes a query to the procedure with {@link String} result.
   *
   * @param method Name of the procedure method to query against.
   * @param arguments Arguments to be submitted to the procedure for the query.
   * @return A {@link String} representing the response.
   * @throws java.io.IOException When there is error querying the procedure.
   */
  String query(String method, Map<String, String> arguments) throws IOException;
}
