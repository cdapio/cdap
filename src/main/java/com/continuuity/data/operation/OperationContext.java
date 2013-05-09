package com.continuuity.data.operation;

/**
 * Defines Operation Context.
 */
public class OperationContext {

  private String application;

  private String account;

  /**
   * Constructor for operation context.
   * @param account  account Id
   * @param application application id
   */
  public OperationContext(String account, String application) {

    if (account == null) {
      throw new IllegalArgumentException("account cannot be null");
    }
    if (account.isEmpty()) {
      throw new IllegalArgumentException("account cannot be empty");
    }
    if (application != null && application.isEmpty()) {
      throw new IllegalArgumentException("application cannot be empty");
    }
    this.account = account;
    this.application = application;
  }

  /**
   * Constructor for operation context.
   * @param account  account Id
   */
  public OperationContext(String account) {
    this(account, null);
  }

  /**
   * @return String application id
   */
  public String getApplication() {
    return this.application;
  }

  /**
   * @return String account Id
   */
  public String getAccount() {
    return account;
  }
}
