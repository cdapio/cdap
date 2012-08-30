package com.continuuity.api.data;

public class OperationResult<ReturnType> {

  /** whether the result has a value */
  boolean empty;

  /** a message explaining the status code */
  String message;

  /** a status code indicating the reason for the value or being empty */
  int statusCode;

  /** the actual return value */
  ReturnType value;

  public OperationResult(ReturnType value) {
    this.value = value;
    this.empty = false;
    this.message = "Success.";
    this.statusCode = StatusCode.OK;
  }

  public OperationResult(ReturnType value, int statusCode) {
    this.value = value;
    this.empty = false;
    this.message = "Success.";
    this.statusCode = statusCode;
  }

  public OperationResult(ReturnType value, int statusCode, String message) {
    this.value = value;
    this.empty = false;
    this.message = message;
    this.statusCode = statusCode;
  }

  public OperationResult(int statusCode, String message) {
    this.value = null;
    this.empty = true;
    this.message = message;
    this.statusCode = statusCode;
  }

  public OperationResult(int statusCode) {
    this.value = null;
    this.empty = true;
    this.message = "Success.";
    this.statusCode = statusCode;
  }

  public boolean isEmpty() {
    return this.empty;
  }

  public String getMessage() {
    return this.message;
  }

  public int getStatus() {
    return this.statusCode;
  }

  public ReturnType getValue() {
    return this.value;
  }

}

