/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal.db;

import java.sql.Timestamp;

/**
 * Stores all DB tables and columns in a central place so that can be used everywhere.
 */
public class DBUtils {

  /**
   * Account Table
   */
  public static class AccountTable {

    public static final String TABLE_NAME = "account";
    public static final String ID_COLUMN = "id";
    public static final String EMAIL_COLUMN = "email_id";
    public static final String FIRST_NAME_COLUMN = "first_name";
    public static final String LAST_NAME_COLUMN = "last_name";
    public static final String COMPANY_COLUMN = "company";
    public static final String CONFIRMED_COLUMN = "confirmed";
    public static final String PASSWORD_COLUMN = "password";
    public static final String API_KEY_COLUMN = "api_key";
    public static final String ACCOUNT_CREATED_AT = "account_created_at";
    public static final String DEV_SUITE_DOWNLOADED_AT = "dev_suite_downloaded_at";
    public static final int ACCOUNT_UNCONFIRMED = 0;
    public static final int ACCOUNT_CONFIRMED = 1;
  }

  /**
   * Account payment table
   * TODO: This is not being used now
   */
  public static class AccountPayment {

    public static final String TABLE_NAME = "account_payment";
    public static final String ACCOUNT_ID_COLUMN = "account_id";
    public static final String CREDIT_CARD_NUMBER_COLUMN = "credit_card_number";
    public static final String CREDIT_CARD_NAME_COLUMN = "credit_card_name";
    public static final String CREDIT_CARD_CVV_COLUMN = "credit_card_cvv";
    public static final String CREDIT_CARD_EXPIRY_COLUMN = "credit_card_expiration";

  }

  /**
   * Defines RoleType for account
   * TODO: This is not being used now
   */
  public static class AccountRoleType {
    public static final String TABLE_NAME = "account_role";
    public static final String ACCOUNT_ID_COLUMN = "account_id";
    public static final String ROLE_NAME_COLUMN = "name";
    public static final String PERMISSIONS_COLUMN = "permissions";
  }

  /**
   * Defines VPC
   */
  public static class VPC {
    public static final String TABLE_NAME = "vpc_account";
    public static final String VPC_ID_COLUMN = "id";
    public static final String ACCOUNT_ID_COLUMN = "account_id";
    public static final String NAME_COLUMN = "vpc_name";
    public static final String LABEL_COLUMN = "vpc_label";
    public static final String VPC_CREATED_AT = "vpc_created_at";
  }

  /**
   * Roles for VPC
   * TODO: Note: This is not being used now
   */
  public static class VPCRole {
    public static final String TABLE_NAME = "vpc_role";
    public static final String VPC_ID_COLUMN = "vpc_id";
    public static final String ACCOUNT_ID_COLUMN = "account_id";
    public static final String USER_ID_COLUMN = "user_id";
    public static final String ROLE_TYPE_COLUMN = "role_type";
    public static final String ROLE_OVERRIDES_COLUMN = "role_overrides";
  }

  /**
   * Store nonce values for session and activation
   */
  public static class Nonce {
    public static final String TABLE_NAME = "nonce";
    public static final String NONCE_ID_COLUMN = "nonce_id";
    public static final String ID_COLUMN = "id";
    public static final String NONCE_EXPIRES_AT_COLUMN = "nonce_expires_at";

  }

  /**
   * Stores list of profane words
   */
  public static class Profanity {
    public static final String TABLE_NAME = "profane_list";
    public static final String PROFANE_WORDS = "name";
  }


  public static long timestampToLong(Timestamp time) {
    if (time == null) {
      return -1;
    }
    return time.getTime();
  }

}
