package com.continuuity.passport.dal.db;

/**
 *
 */
public class DBUtils {

  public static class AccountTable {

    public static final String TABLE_NAME = "account";
    public static  final String ID_COLUMN = "id";
    public static final String EMAIL_COLUMN ="email_id";
    public static final String FIRST_NAME_COLUMN = "first_name";
    public static final String LAST_NAME_COLUMN = "last_name";
    public static final String COMPANY_COLUMN = "company";
    public static final String CONFIRMED_COLUMN = "confirmed";
    public static final String PASSWORD_COLUMN = "password";
    public static final String API_KEY_COLUMN = "api_key";
    public static final int ACCOUNT_UNCONFIRMED = 0;
    public static final int ACCOUNT_CONFIRMED = 1;
  }

  public static class AccountPayment {

    public static final String TABLE_NAME = "account_payment";
    public static final String ACCOUNT_ID_COLUMN = "account_id";
    public static final String CREDIT_CARD_NUMBER_COLUMN = "credit_card_number" ;
    public static final String CREDIT_CARD_NAME_COLUMN = "credit_card_name" ;
    public static final String CREDIT_CARD_CVV_COLUMN= "credit_card_cvv" ;
    public static final String CREDIT_CARD_EXPIRY_COLUMN = "credit_card_expiration" ;

  }

  public static class AccountRoleType {
    public static final String TABLE_NAME = "account_role";
    public static final String ACCOUNT_ID_COLUMN = "account_id";
    public static final String ROLE_NAME_COLUMN = "name";
    public static final String PERMISSIONS_COLUMN= "permissions";
  }

  public static class VPC {
    public static final String TABLE_NAME = "vpc_account";
    public static final String VPC_ID_COLUMN = "id";
    public static final String ACCOUNT_ID_COLUMN = "account_id";
    public static final String NAME_COLUMN = "vpc_name";
  }

  public static class VPCRole {
    public static final String TABLE_NAME = "vpc_role";
    public static final String VPC_ID_COLUMN = "vpc_id";
    public static final String ACCOUNT_ID_COLUMN = "account_id";
    public static final String USER_ID_COLUMN = "user_id"; //TODO: Name better!
    public static final String ROLE_TYPE_COLUMN = "role_type";
    public static final String ROLE_OVERRIDES_COLUMN = "role_overrides";
  }


}
