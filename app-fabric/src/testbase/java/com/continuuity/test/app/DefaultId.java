package com.continuuity.test.app;

import com.continuuity.app.Id;
import com.continuuity.test.data.Constants;

/**
 * Default Ids to use in test if you do not want to construct your own.
 */
public class DefaultId {
  private static final String DEFAULT_ACCOUNT_ID = Constants.DEFAULT_ACCOUNT_ID;
  private static final String DEFAULT_APPLICATION_ID = "myapp";
  private static final String DEFAULT_PROGRAM_ID = "pgm";

  public static final Id.Account ACCOUNT = new Id.Account(DEFAULT_ACCOUNT_ID);
  public static final Id.Application APPLICATION = new Id.Application(ACCOUNT, DEFAULT_APPLICATION_ID);
  public static final Id.Program PROGRAM = new Id.Program(APPLICATION, DEFAULT_PROGRAM_ID);
}
