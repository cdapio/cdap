package com.continuuity.passport.core.meta;

/**
 *
 */
public class AccountSecurity {
  private final Account account;
  private final String password;


  public AccountSecurity(Account account, String password) {
    this.account = account;
    this.password = password;
  }

  public Account getAccount() {
    return account;
  }

  public String getPassword() {
    return password;
  }
}
