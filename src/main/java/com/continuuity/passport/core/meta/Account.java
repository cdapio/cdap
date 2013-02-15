package com.continuuity.passport.core.meta;

import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;

/**
 * Defines account
 */
public class Account {

  private final String first_name;

  private final String last_name;

  private final String company;

  private final  String email_id;

  private final int account_id;

  private final String api_key;

  public Account(String firstName, String lastName, String emailId) {
    this(firstName,lastName, StringUtils.EMPTY,emailId,-1);
  }

  public Account(String firstName, String lastName, String company, String emailId, int accountId) {
    this(firstName,lastName,company,emailId,accountId,StringUtils.EMPTY);
  }

  public Account(String firstName, String lastName, String company, String emailId, int accountId,String apiKey) {
    this.first_name = firstName;
    this.last_name = lastName;
    this.company = company;
    this.email_id = emailId;
    this.account_id = accountId;
    this.api_key = apiKey;
  }


  public Account(String firstName, String lastName, String company, String emailId) {
    this(firstName,lastName,company,emailId,-1);
  }


  public String getFirstName() {
    return first_name;
  }

  public String getLastName() {
    return last_name;
  }

  public String getCompany() {
    return company;
  }

  public String getEmailId() {
    return email_id;
  }

  public int getAccountId() {
    return account_id;
  }

  public String toString() {
    Gson gson = new Gson();
    return (gson.toJson(this));

  }

}

