package com.continuuity.api.data;

public class OperationException extends Exception {

  int statusCode;

  public OperationException(int statusCode, String message) {
    super(message);
    this.statusCode = statusCode;
  }

  public OperationException(int statusCode, String message, Throwable cause) {
    super(message, cause);
    this.statusCode = statusCode;
  }

  public int getStatus() {
    return this.statusCode;
  }

}