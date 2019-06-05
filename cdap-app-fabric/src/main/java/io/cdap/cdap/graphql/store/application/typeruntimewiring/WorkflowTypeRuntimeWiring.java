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

package io.cdap.cdap.graphql.store.application.typeruntimewiring;

import graphql.schema.idl.TypeRuntimeWiring;
import io.cdap.cdap.graphql.store.application.datafetchers.ScheduleDataFetcher;
import io.cdap.cdap.graphql.store.application.schema.ApplicationFields;
import io.cdap.cdap.graphql.store.application.schema.ApplicationTypes;
import io.cdap.cdap.graphql.typeruntimewiring.CDAPTypeRuntimeWiring;

/**
 * ApplicationQuery type runtime wiring. Registers the data fetchers for the ApplicationQuery type.
 */
public class WorkflowTypeRuntimeWiring implements CDAPTypeRuntimeWiring {

  private static final WorkflowTypeRuntimeWiring INSTANCE = new WorkflowTypeRuntimeWiring();

  private final ScheduleDataFetcher scheduleDataFetcher;

  private WorkflowTypeRuntimeWiring() {
    this.scheduleDataFetcher = ScheduleDataFetcher.getInstance();
  }

  public static WorkflowTypeRuntimeWiring getInstance() {
    return INSTANCE;
  }

  @Override
  public TypeRuntimeWiring getTypeRuntimeWiring() {
    return TypeRuntimeWiring.newTypeWiring(ApplicationTypes.WORKFLOW)
      .dataFetcher("startTimes", scheduleDataFetcher.getNextRuntimesDataFetcher())
      .dataFetcher("runs", scheduleDataFetcher.getsome())
      .build();
  }

}
