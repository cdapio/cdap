package com.continuuity.api.data;

public class OperationContext {

  private String application;
  private String account;

  public OperationContext(String account, String application) {

    if (account == null)
      throw new IllegalArgumentException("account cannot be null");
    if (account.isEmpty())
      throw new IllegalArgumentException("account cannot be empty");
    if (application != null && application.isEmpty())
      throw new IllegalArgumentException("application cannot be empty");

    this.account = account;
    this.application = application;
  }

  public OperationContext(String account) {
    this(account, null);
  }

  public String getApplication() {
    return this.application;
  }

  public String getAccount() {
    return account;
  }

  /** defaults to be used everywhere until we support true accounts */
  public static final String DEFAULT_ACCOUNT_ID =
      "demo";
  public static final OperationContext DEFAULT =
      new OperationContext(DEFAULT_ACCOUNT_ID);
}
