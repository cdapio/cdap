/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.explore.service;

import java.util.List;

/**
 * Hold the results of a statement execution as well as the schema of the results.
 */
public class ResultWithSchema {
  private final List<Result> results;
  private final List<ColumnDesc> schema;

  public ResultWithSchema(List<Result> results, List<ColumnDesc> schema) {
    this.results = results;
    this.schema = schema;
  }


  public List<Result> getResults() {
    return results;
  }

  public List<ColumnDesc> getSchema() {
    return schema;
  }
}
