package com.continuuity.data.operation;

/**
 * Defines internal status codes for use in operation results or execptions.
 */
public class StatusCode extends com.continuuity.api.data.StatusCode {

  public static final int QUEUE_NOT_FOUND = 1000;
//  public static final int QUEUE_EMPTY = 1001;
  public static final int ILLEGAL_ACK = 1002;
//  public static final int TOO_MANY_RETRIES = 1003;
  public static final int ILLEGAL_GROUP_CONFIG_CHANGE = 1004;
  public static final int ILLEGAL_FINALIZE = 1005;
  public static final int ILLEGAL_UNACK = 1006;
  public static final int NOT_CONFIGURED = 1007;
  public static final int INVALID_STATE = 1008;

  public static final int INVALID_TRANSACTION = 1009;

  public static final int SQL_ERROR = 5001;
  public static final int HBASE_ERROR = 5002;
  public static final int THRIFT_ERROR = 5003;
}
