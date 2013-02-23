package com.continuuity.app;

/**
 * Default Ids to use in test
 */
public class DefaultId {
  private static String DEFAULT_ACCOUNT_ID = "default";
  private static String DEFAULT_APPLICATION_ID = "myapp";
  private static String DEFAULT_PROGRAM_ID = "pgm";

  public static Id.Account ACCOUNT = new Id.Account(DEFAULT_ACCOUNT_ID);
  public static Id.Application APPLICATION = new Id.Application(ACCOUNT, DEFAULT_APPLICATION_ID);
  public static Id.Program PROGRAM = new Id.Program(APPLICATION, DEFAULT_PROGRAM_ID);
}
