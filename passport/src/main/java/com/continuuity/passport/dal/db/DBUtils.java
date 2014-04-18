/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal.db;

import java.sql.Timestamp;

/**
 * Stores all DB tables and columns in a central place so that can be used everywhere.
 */
public class DBUtils {

  public static final String DB_INTEGRITY_CONSTRAINT_VIOLATION = "23000";
  public static final String DB_INTEGRITY_CONSTRAINT_VIOLATION_DUP_KEY = "23505";
  public static final String DB_INTEGRITY_CONSTRAINT_VIOLATION_FOR_KEY = "23503";

  /**
   * Represents schema corresponding to 'account' table in the db.
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
    public static final String PAYMENT_INFO_PROVIDED_AT = "payment_info_provided_at";
    public static final String PAYMENT_ACCOUNT_ID = "payment_account_id";
    public static final String ORG_ID = "org_id";
    public static final int ACCOUNT_UNCONFIRMED = 0;
    public static final int ACCOUNT_CONFIRMED = 1;
  }

  /**
   * Account payment table.
   * TODO: (ENG-2215) - Cleanup interfaces
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
   * Represents schema corresponding to 'account_role' table in the db.
   */
  public static class AccountRoleType {
    public static final String TABLE_NAME = "account_role";
    public static final String ACCOUNT_ID_COLUMN = "account_id";
    public static final String ROLE_NAME_COLUMN = "name";
    public static final String PERMISSIONS_COLUMN = "permissions";
  }

  /**
   * Represents schema corresponding to 'vpc_account' table in the db.
   */
  public static class VPC {
    public static final String TABLE_NAME = "vpc_account";
    public static final String VPC_ID_COLUMN = "id";
    public static final String ACCOUNT_ID_COLUMN = "account_id";
    public static final String NAME_COLUMN = "vpc_name";
    public static final String LABEL_COLUMN = "vpc_label";
    public static final String VPC_CREATED_AT = "vpc_created_at";
    public static final String VPC_TYPE = "vpc_type";
  }

  /**
   * Represents schema corresponding to 'vpc' table in the db.
   * */
  public static class VPCRole {
    public static final String TABLE_NAME = "vpc_roles";
    public static final String VPC_ID_COLUMN = "vpc_id";
    public static final String ACCOUNT_ID_COLUMN = "account_id";
    public static final String USER_ID_COLUMN = "user_id";
    public static final String ROLE_TYPE_COLUMN = "role_type";
    public static final String ROLE_OVERRIDES_COLUMN = "role_overrides";
  }

  /**
   * Represents schema corresponding to 'nonce' table in the db.
   */
  public static class Nonce {
    public static final String TABLE_NAME = "nonce";
    public static final String NONCE_ID_COLUMN = "nonce_id";
    public static final String ID_COLUMN = "id";
    public static final String NONCE_EXPIRES_AT_COLUMN = "nonce_expires_at";

  }

  /**
   * Represents schema corresponding to organization table.
   */
  public static class Organization {
    public static final String TABLE_NAME = "organization";
    public static final String ID = "id";
    public static final String NAME = "name";
  }

  public static long timestampToLong(Timestamp time) {
    if (time == null) {
      return -1;
    }
    return time.getTime();
  }

}
