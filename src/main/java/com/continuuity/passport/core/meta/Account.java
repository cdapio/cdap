package com.continuuity.passport.core.meta;

import com.google.gson.Gson;

/**
 * Defines account
 */
public class Account {
  private String name;

  private String emailId;

  private int accountId;

  public Account(String name, String emailId, int accountId) {
    this.name = name;
    this.emailId = emailId;
    this.accountId = accountId;
  }


  public Account(String name, String emailId) {
    this(name,emailId,-1);
  }

  public String getName() {
    return name;
  }

  public String getEmailId() {
    return emailId;
  }

  public int getAccountId() {
    return accountId;
  }

  public String toString() {
    Gson gson = new Gson();
    return (gson.toJson(this));

  }

}

