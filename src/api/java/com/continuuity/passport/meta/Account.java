/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.meta;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Defines account
 */
public class Account {

  private final String firstName;

  private final String lastName;

  private final String company;

  private final String emailId;

  private final int accountId;

  private final String apiKey;

  private final boolean confirmed;

  private final long devSuiteDownloadTime;

  public static Account fromString(String jsonString) {
    Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
    return gson.fromJson(jsonString, Account.class);
  }

  public Account(String firstName, String lastName, String emailId) {
    this(firstName, lastName, "", emailId, -1);
  }

  public Account(String firstName, String lastName, String company, String emailId, int accountId) {
    this(firstName, lastName, company, emailId, accountId,"", false, -1);
  }

  public Account(String firstName, String lastName, String company, String emailId,
                 int accountId, String apiKey, boolean confirmed , long devSuiteDownloadTime) {
    this.firstName = firstName;
    this.lastName = lastName;
    this.company = company;
    this.emailId = emailId;
    this.accountId = accountId;
    this.apiKey = apiKey;
    this.confirmed = confirmed;
    this.devSuiteDownloadTime = devSuiteDownloadTime;
  }

  public Account(String firstName, String lastName, String company, int accountId) {
    this(firstName, lastName, company, "", accountId);
  }


  public Account(String firstName, String lastName, String company, String emailId) {
    this(firstName, lastName, company, emailId, -1);
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

  public String getApiKey() {
    return apiKey;
  }

  @Override
  public String toString() {
    Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
    return gson.toJson(this);
  }

}

