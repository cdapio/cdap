package com.continuuity.test.internal;

import com.continuuity.app.Id;

/**
 * Default Ids to use in test if you do not want to construct your own.
 */
public class DefaultId {
  public static final String DEFAULT_ACCOUNT_ID = "developer"; // changed from default
  private static final String DEFAULT_APPLICATION_ID = "myapp";
  private static final String DEFAULT_PROGRAM_ID = "pgm";

  public static final Id.Account ACCOUNT = new Id.Account(DEFAULT_ACCOUNT_ID);
  public static final Id.Application APPLICATION = new Id.Application(ACCOUNT, DEFAULT_APPLICATION_ID);
  public static final Id.Program PROGRAM = new Id.Program(APPLICATION, DEFAULT_PROGRAM_ID);
}
