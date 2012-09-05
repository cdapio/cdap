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

  public String getStatusMessage() {
    return String.format("[%d] %s", this.statusCode, super.getMessage());
  }

  @Override
  public String getMessage() {
    return getStatusMessage();
  }

  public String getMessageOnly() {
    return super.getMessage();
  }
}