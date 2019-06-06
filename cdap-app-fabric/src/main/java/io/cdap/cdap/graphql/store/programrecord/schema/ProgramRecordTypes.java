/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.graphql.store.programrecord.schema;

import io.cdap.cdap.graphql.schema.Types;

/**
 * Helper class with a collection of fields relevant to program records that are used in the server
 */
public class ProgramRecordTypes implements Types {

  public static final String WORKFLOW = "Workflow";
  public static final String PROGRAM_RECORD = "ProgramRecord";
  public static final String MAP_REDUCE = "MapReduce";

  private ProgramRecordTypes() {
    throw new UnsupportedOperationException("Helper class should not be instantiated");
  }

}
