package com.continuuity.app.runtime;

/**
 *
 */
public interface ProgramOptions {

  String getName();

  Arguments getArguments();

  Arguments getUserArguments();

  boolean isDebug();
}
