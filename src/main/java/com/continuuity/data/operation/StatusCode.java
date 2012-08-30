package com.continuuity.data.operation;

public class StatusCode extends com.continuuity.api.data.StatusCode {

  public static final int QUEUE_NOT_FOUND = 1000;
  public static final int QUEUE_EMPTY = 1001;
  public static final int ILLEGAL_ACK = 1002;
  public static final int TOO_MANY_RETRIES = 1003;

  public static final int INTERNAL_ERROR = 5000;

}
