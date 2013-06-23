package com.continuuity.passport.http.client;

import com.continuuity.passport.meta.Account;

/**
 * Overriding accountId type to String. Ideally Account should use String type for accountId.
 * TODO: (ENG-2213) - Use String for accountId remove AccountProvider class                 .
 * @param <T> specific {@code Account} Type
 */
public class AccountProvider<T extends Account> {
  private final T t;

  /**
   * Construct account provider specific {@code Account} Type.
   * @param t Instance of {@code Account} Type
   */
  public AccountProvider(T t) {
    this.t = t;
  }

  /**
   * @return instance of {@code Account}
   */
  public T get() {
    return t;
  }

  /**
   * @return String representation of accountId
   */
  public String getAccountId() {
    return String.format("%s", t.getAccountId());
  }
}
