/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.data2;

import com.continuuity.data.operation.StatusCode;

/**
 * This class is used to return results of data operations. It allows to return "nothing" with a status code and a
 * descriptive message why there is no result, rather than just returning null (which is also ambiguous, because it
 * may mean that there is a result with the value null).
 * @param <ReturnType> The type of the actual return value.
 */
public class OperationResult<ReturnType> {

  /**
   * Whether the result has a value.
   */
  boolean empty;

  /**
   * A message explaining the status code.
   */
  String message;

  /**
   * A status code indicating the reason for the value or being empty.
   */
  int statusCode;

  /**
   * The actual return value.
   */
  ReturnType value;

  /**
   * Constructor with an actual return value. In this case, the status code defaults to OK,
   * and the message defaults to "Success.".
   * @param value the return value
   */
  public OperationResult(ReturnType value) {
    this.value = value;
    this.empty = false;
    this.message = "Success.";
    this.statusCode = StatusCode.OK;
  }

  /**
   * Constructor for the case where there is no result, with a status code and message indicating why.
   * @param statusCode the status code
   * @param message a message describing why there is no result
   */
  public OperationResult(int statusCode, String message) {
    this.value = null;
    this.empty = true;
    this.message = message;
    this.statusCode = statusCode;
  }

  /**
   * Constructor for the case where there is no result, with a status code, and the message "Not Found.".
   * @param statusCode the status code
   */
  public OperationResult(int statusCode) {
    this.value = null;
    this.empty = true;
    this.message = "Not Found.";
    this.statusCode = statusCode;
  }

  /**
   * Whether there was a result.
   * @return true is there is no result
   */
  public boolean isEmpty() {
    return this.empty;
  }

  /**
   * Get the message.
   * @return the message
   */
  public String getMessage() {
    return this.message;
  }

  /**
   * Get the status code.
   * @return the status code
   */
  public int getStatus() {
    return this.statusCode;
  }

  /**
   * Get the actual return value. If isEmpty() is true, then this is null.
   * @return the return value
   */
  public ReturnType getValue() {
    return this.value;
  }

}

