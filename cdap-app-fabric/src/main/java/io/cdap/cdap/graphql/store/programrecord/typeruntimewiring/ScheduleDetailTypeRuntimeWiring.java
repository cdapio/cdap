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

package io.cdap.cdap.graphql.store.programrecord.typeruntimewiring;

import graphql.schema.idl.TypeRuntimeWiring;
import io.cdap.cdap.graphql.store.programrecord.datafetchers.ScheduleDataFetcher;
import io.cdap.cdap.graphql.store.programrecord.schema.ProgramRecordFields;
import io.cdap.cdap.graphql.store.programrecord.schema.ProgramRecordTypes;
import io.cdap.cdap.graphql.typeruntimewiring.CDAPTypeRuntimeWiring;

/**
 * ScheduleDetail type runtime wiring. Registers the data fetchers for the ScheduleDetail type.
 */
public class ScheduleDetailTypeRuntimeWiring implements CDAPTypeRuntimeWiring {

  private static final ScheduleDetailTypeRuntimeWiring INSTANCE = new ScheduleDetailTypeRuntimeWiring();

  private final ScheduleDataFetcher scheduleDataFetcher;

  private ScheduleDetailTypeRuntimeWiring() {
    this.scheduleDataFetcher = ScheduleDataFetcher.getInstance();
  }

  public static ScheduleDetailTypeRuntimeWiring getInstance() {
    return INSTANCE;
  }

  @Override
  public TypeRuntimeWiring getTypeRuntimeWiring() {
    return TypeRuntimeWiring.newTypeWiring(ProgramRecordTypes.SCHEDULE_DETAIL)
      .dataFetcher(ProgramRecordFields.TIME, scheduleDataFetcher.getTimeDataFetcher())
      .build();
  }

}
