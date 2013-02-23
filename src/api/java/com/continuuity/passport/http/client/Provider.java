package com.continuuity.passport.http.client;

import com.continuuity.passport.meta.Account;

/**
 *
 */
public class Provider<T extends Account> {
  private final T t;

  public Provider(T t) {
    this.t = t;
  }

  public T get() {
    return t;
  }

  public String getAccountId() {
    return String.format("%s", t.getAccountId());
  }
}
