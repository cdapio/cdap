package com.continuuity.data2;

/**
 * Defines OperationException.
 */
public class OperationException extends Exception {

  int statusCode;

  /**
   * Constructor for operation exception.
   * @param statusCode status code
   * @param message a descriptive message for the error
   */
  public OperationException(int statusCode, String message) {
    super(message);
    this.statusCode = statusCode;
  }

  /**
   * Constructor for operation exception.
   * @param statusCode status code
   * @param message a descriptive message for the error
   * @param cause the original throwable that caused this error
   */
  public OperationException(int statusCode, String message, Throwable cause) {
    super(message, cause);
    this.statusCode = statusCode;
  }

  /**
   * Get the status code.
   * @return status code
   */
  public int getStatus() {
    return this.statusCode;
  }

  /**
   * Get the status message.
   * @return the status message
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
