package com.continuuity.api.data;

/**
 * Defines OperationException
 */
public class OperationException extends Exception {

  int statusCode;

  /**
   * Constructor for operation exception
   * @param statusCode status code
   * @param message Exception message
   */
  public OperationException(int statusCode, String message) {
    super(message);
    this.statusCode = statusCode;
  }

  /**
   * Constructor for operation exception
   * @param statusCode status code
   * @param message message
   * @param cause  Throwable cause
   */
  public OperationException(int statusCode, String message, Throwable cause) {
    super(message, cause);
    this.statusCode = statusCode;
  }

  /**
   * get Status code
   * @return integer status code
   */
  public int getStatus() {
    return this.statusCode;
  }

  /**
   * get status message
   * @return String message
   */
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