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

import io.cdap.cdap.graphql.schema.Fields;

/**
 * Helper class with a collection of fields relevant to program records that are used in the server
 */
public class ProgramRecordFields implements Fields {

  public static final String START_TIMES = "startTimes";
  public static final String RUNS = "runs";
  public static final String TYPE = "type";
  public static final String DESCRIPTION = "description";
  public static final String APP = "app";
  public static final String PID = "pid";
  public static final String START_TS = "startTs";
  public static final String RUN_TS = "runTs";
  public static final String STOP_TS = "stopTs";
  public static final String SUSPEND_TS = "suspendTs";
  public static final String RESUME_TS = "resumeTs";
  public static final String STATUS = "status";
  public static final String PROFILE_ID = "profileId";

  private ProgramRecordFields() {
    throw new UnsupportedOperationException("Helper class should not be instantiated");
  }

}
