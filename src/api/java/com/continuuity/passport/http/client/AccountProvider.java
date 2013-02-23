package com.continuuity.passport.http.client;

import com.continuuity.passport.meta.Account;

/**
 * This is a hack for overriding accountId type to String.
 * Ideally Account should use String type for account id instead.
 */
public class AccountProvider<T extends Account> {
  private final T t;

  public AccountProvider(T t) {
    this.t = t;
  }

  public T get() {
    return t;
  }

  public String getAccountId() {
    return String.format("%s", t.getAccountId());
  }
}
