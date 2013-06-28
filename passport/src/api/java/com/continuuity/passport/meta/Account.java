/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.meta;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Representation of Account entity in the continuuity system.
 */
public class Account {

  private final String firstName;

  private final String lastName;

  private final String company;

  private final String emailId;

  private final int accountId;

  private final String apiKey;

  private final boolean confirmed;

  private final long devsuiteDownloadTime;

  private final long paymentInfoProvidedAt;

  private final String paymentAccountId;

  private final String orgId;

  /**
   * Deserialize Account from jsonString.
   * @param jsonString json string containing account info
   * @return {@code Account}
   */
  public static Account fromString(String jsonString) {
    Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
    return gson.fromJson(jsonString, Account.class);
  }

  /**
   * Construct account object with firstName, lastName, emailId.
   * @param firstName firstName
   * @param lastName lastName
   * @param emailId  EmailId
   */
  public Account(String firstName, String lastName, String emailId) {
    this(firstName, lastName, "", emailId, -1);
  }

  /**
   * Construct account object with firstName, lastName, emailId , company, accountId.
   * @param firstName firstName
   * @param lastName lastName
   * @param emailId  EmailId
   * @param company Company
   * @param accountId  AccountID
   */
  public Account(String firstName, String lastName, String company, String emailId, int accountId) {
    this(firstName, lastName, company, emailId, accountId, "", false, -1);
  }

  /**
   * Construct account object with first name, last name, email id, company, account id, api key, confirmed flag
   * and dev suite downloaded time.
   * @param firstName firstName
   * @param lastName lastName
   * @param emailId  EmailId
   * @param company Company
   * @param accountId  AccountID
   * @param apiKey  apikey
   * @param confirmed boolean flag that shows if user has confirmed registration
   * @param devSuiteDownloadTime DevSuite downloaded time
   */
  public Account(String firstName, String lastName, String company, String emailId,
                 int accountId, String apiKey, boolean confirmed, long devSuiteDownloadTime) {
    this(firstName, lastName, company, emailId, accountId, apiKey, confirmed, devSuiteDownloadTime, -1, "", null);

  }

  /**
   * Construct account object with first name, last name, email id, company, account id, api key, confirmed flag,
   * dev suite downloaded time, external payment Id and payment info provided time.
   * @param firstName firstName
   * @param lastName lastName
   * @param emailId  EmailId
   * @param company Company
   * @param accountId  AccountID
   * @param apiKey  apikey
   * @param confirmed boolean flag that shows if user has confirmed registration
   * @param devSuiteDownloadTime DevSuite downloaded time
   * @param paymentInfoProvidedAt time when payment info was provided
   * @param paymentAccountId external payment id
   * @param orgId organization id of the account
   */
  public Account(String firstName, String lastName, String company, String emailId, int accountId,  String apiKey,
                 boolean confirmed, long devSuiteDownloadTime, long paymentInfoProvidedAt, String paymentAccountId,
                 String orgId) {
    this.firstName = firstName;
    this.lastName = lastName;
    this.company = company;
    this.emailId = emailId;
    this.accountId = accountId;
    this.apiKey = apiKey;
    this.confirmed = confirmed;
    this.devsuiteDownloadTime = devSuiteDownloadTime;
    this.paymentInfoProvidedAt = paymentInfoProvidedAt;
    this.paymentAccountId = paymentAccountId;
    this.orgId = orgId;
  }

  /**
   * Construct account object with first name, last name, email id, company, account id.
   * @param firstName firstName
   * @param lastName lastName
   * @param company Company
   * @param accountId  AccountID
   */
  public Account(String firstName, String lastName, String company, int accountId) {
    this(firstName, lastName, company, "", accountId);
  }

  /**
   * Construct account object with first name, last name, email id, company.
   * @param firstName firstName
   * @param lastName lastName
   * @param emailId  EmailId
   * @param company Company
   */
  public Account(String firstName, String lastName, String company, String emailId) {
    this(firstName, lastName, company, emailId, -1);
  }

  /**
   * Get first name of the account holder.
   * @return String firstName
   */
  public String getFirstName() {
    return firstName;
  }

  /**
   * Get last name of the account holder.
   * @return String lastName
   */
  public String getLastName() {
    return lastName;
  }

  /**
   * Get Company of the account holder.
   * @return String company
   */
  public String getCompany() {
    return company;
  }

  /**
   * Get EmailId of the account holder.
   * @return String emailId
   */
  public String getEmailId() {
    return emailId;
  }

  /**
   * Get Account ID.
   * @return account id
   */
  public int getAccountId() {
    return accountId;
  }

  /**
   * Get APIKey of the account.
   * @return  apiKey
   */
  public String getApiKey() {
    return apiKey;
  }

  /**
   * Get Id in the external payment system.
   * @return payment ID
   */
  public String getPaymentAccountId() {
    return paymentAccountId;
  }

  /**
   * @return organization id of the account.
   */
  public String getOrgId() {
    return orgId;
  }

  /**
   * @return {@code JsonElement} Json representation of Account object
   */
  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("id", getAccountId());
    object.addProperty("first_name", getFirstName());
    object.addProperty("last_name", getLastName());
    object.addProperty("company", getCompany());
    object.addProperty("email_id", getEmailId());
    object.addProperty("api_key", getApiKey());
    return object;
  }

  /**
   * Serialize account into json string. The field names will be lowercase with underscores where captialized.
   * @return Json serialized String representing account
   */
  @Override
  public String toString() {
    Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
    return gson.toJson(this);

  }

}

