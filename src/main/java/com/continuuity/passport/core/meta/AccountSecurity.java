package com.continuuity.passport.core.meta;

/**
*
*/
public class AccountSecurity {
  private final int account_id;
  private final String password;


  public AccountSecurity(int accountId, String password) {
    this.account_id = accountId;
    this.password = password;
  }

  public int getAccountId() {
    return account_id;
  }

  public String getPassword() {
    return password;
  }
}
