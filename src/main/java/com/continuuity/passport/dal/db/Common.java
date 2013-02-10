package com.continuuity.passport.dal.db;

/**
 *
 */
public class Common {

  public static class AccountTable {

    public static final String TABLE_NAME = "account";

    public static  final String ID_COLUMN = "id";

    public static final String EMAIL_COLUMN ="email_id";

    public static final String NAME_COLUMN = "name";

    public static final String CONFIRMED_COLUMN = "confirmed";

    public static final String PASSWORD_COLUMN = "password";

    public static final String API_KEY_COLUMN = "api_key";

    public static final int ACCOUNT_UNCONFIRMED = 0;

    public static final int ACCOUNT_CONFIRMED = 1;

  }

  public static class AccountPayment {

    public static final String TABLE_NAME = "account_payment";

    public static final String ACCOUNT_ID = "account_id";

    public static final String CREDIT_CARD_NUMBER = "credit_card_number" ;

    public static final String CREDIT_CARD_NAME = "credit_card_name" ;

    public static final String CREDIT_CARD_CVV= "credit_card_cvv" ;

    public static final String CREDIT_CARD_EXPIRY = "credit_card_expiration" ;

  }
}
