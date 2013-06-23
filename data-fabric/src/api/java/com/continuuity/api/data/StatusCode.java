package com.continuuity.api.data;

/**
 * Defines public status codes for use in operation results or execptions.
 */
public class StatusCode {

  public static final int OK = 0;

  public static final int WRITE_CONFLICT = 500;

  public static final int ENTRY_NOT_FOUND = 501;

  public static final int ENTRY_EXISTS = 502;

  public static final int TRANSACTION_CONFLICT = 503;

  public static final int KEY_NOT_FOUND = 404;

  public static final int COLUMN_NOT_FOUND = 405;

  public static final int ILLEGAL_INCREMENT = 2000;
  public static final int INCOMPATIBLE_TYPE = 2001;

  public static final int INTERNAL_ERROR = 5000;

}
