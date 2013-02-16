package com.continuuity.passport.core.meta;

/**
*
*/
public class AccountSecurity {
  private final int accountId;
  private final String password;


  public AccountSecurity(int accountId, String password) {
    this.accountId = accountId;
    this.password = password;
  }

  public int getAccountId() {
    return accountId;
  }

  public String getPassword() {
    return password;
  }
}
