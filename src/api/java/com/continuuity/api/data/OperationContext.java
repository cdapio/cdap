package com.continuuity.api.data;

public class OperationContext {

  private String application;
  private String account;

  public OperationContext(String account, String application) {
    this.account = account;
    this.application = application;
  }

  public String getApplication() {
    return this.application;
  }

  public String getAccount() {
    return account;
  }

  public static final OperationContext DEFAULT =
      new OperationContext("default", "default");
}
