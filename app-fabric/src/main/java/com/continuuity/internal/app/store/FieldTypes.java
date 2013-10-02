/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
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
