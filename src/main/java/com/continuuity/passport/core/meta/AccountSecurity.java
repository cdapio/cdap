package com.continuuity.passport.core.meta;

/**
*
*/
public class AccountSecurity {
  private Account account;
  private String saltedHashedPassword;

  public AccountSecurity(Account account, String saltedHashedPassword) {
    this.account = account;
    this.saltedHashedPassword = saltedHashedPassword;
  }

  public Account getAccount() {
    return account;
  }

  public String getSaltedHashedPassword() {
    return saltedHashedPassword;
  }
}
