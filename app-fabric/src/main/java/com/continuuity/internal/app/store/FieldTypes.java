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

package com.continuuity.internal.app.store;

/**
 * Constants used to serialize objects when storing into MDTBasedStore
 */
final class FieldTypes {
  public static class ProgramRun {
    public static final String ENTRY_TYPE = "run";
    public static final String RUN_ID = "runid";
    public static final String PROGRAM = "prg";
    public static final String START_TS = "start";
    public static final String END_TS = "end";
    public static final String END_STATE = "stat";
    public static final String ARGS = "args";
  }

  public static class Application {
    public static final String ENTRY_TYPE = "app";
    public static final String SPEC_JSON = "spec";
    public static final String ARCHIVE_LOCATION = "loc";
    public static final String TIMESTAMP = "ts";
  }

  public static class Stream {
    public static final String ENTRY_TYPE = "str";
    public static final String SPEC_JSON = "spec";
  }

  public static class DataSet {
    public static final String ENTRY_TYPE = "ds";
    public static final String SPEC_JSON = "spec";
  }
}
