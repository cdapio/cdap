/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;

/**
 * App with Flow and MapReduce. used for testing MDS.
 */
public class FlowMapReduceApp implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("App")
      .setDescription("Application which has everything")
      .withStreams()
      .add(new Stream("stream"))
      .withDataSets()
       .add(new KeyValueTable("kvt"))
      .withFlows()
        .add(new AllProgramsApp.NoOpFlow())
      .noProcedure()
      .withMapReduce()
        .add(new AllProgramsApp.NoOpMR())
      .noWorkflow()
      .build();
  }
}
