package com.continuuity.data2.datafabric.dataset.service.executor;

/**
 * Used to respond to various admin operation requests.
 */
public final class DatasetAdminOpResponse {

  private Object result;
  private String message;

  public DatasetAdminOpResponse(Object result, String message) {
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
