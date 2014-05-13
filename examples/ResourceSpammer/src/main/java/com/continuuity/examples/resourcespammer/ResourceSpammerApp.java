/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.resourcespammer;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;

/**
 * ResourceSpammer application that demonstrates CPU limits.
 */
public class ResourceSpammerApp implements Application {

  public static final String TABLE_NAME = "randomTable";

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("ResourceSpammerApp")
      .setDescription("Application that uses lots of system resources")
      .noStream()
      .withDataSets()
        .add(new KeyValueTable("input"))
        .add(new KeyValueTable("output"))
      .withFlows()
        .add(new CPUSpammerFlow())
      .withProcedures()
        .add(new CPUSpamProcedure())
      .noMapReduce()
      .noWorkflow()
      .build();
  }
}
