package com.continuuity.passport.core.meta;

import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;

/**
 * Defines account
 */
public class Account {

  private final String firstName;

  private final String lastName;

  private final String company;

  private final  String emailId;

  private final int accountId;

  public Account(String firstName, String lastName, String emailId) {
    this(firstName,lastName, StringUtils.EMPTY,emailId,-1);
  }

  public Account(String firstName, String lastName, String company, String emailId, int accountId) {
    this.firstName = firstName;
    this.lastName = lastName;
    this.company = company;
    this.emailId = emailId;
    this.accountId = accountId;
  }

  public Account(String firstName, String lastName, String company, String emailId) {
    this(firstName,lastName,company,emailId,-1);
  }


  public String getFirstName() {
    return firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public String getCompany() {
    return company;
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

