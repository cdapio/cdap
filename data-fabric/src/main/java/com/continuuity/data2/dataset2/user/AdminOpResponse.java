package com.continuuity.data2.dataset2.user;

/**
 * Used to respond to various admin operation requests.
 */
public final class AdminOpResponse {

  private Object result;
  private String message;

  public AdminOpResponse(Object result, String message) {
    this.result = result;
    this.message = message;
  }

  public Object getResult() {
    return result;
  }

  public String getMessage() {
    return message;
  }
}
