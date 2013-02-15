package com.continuuity.passport.core.meta;

/**
*
*/
public class BillingInfo {

  private final String credit_card_name;
  private final String credit_card_number;
  private final String cvv;
  private final String expiration_date;

  public BillingInfo(String creditCardName, String creditCardNumber, String cvv, String expirationDate) {
    this.credit_card_name = creditCardName;
    this.credit_card_number = creditCardNumber;
    this.cvv = cvv;
    this.expiration_date = expirationDate;
  }

  public String getCreditCardName() {
    return credit_card_name;
  }

  public String getCreditCardNumber() {
    return credit_card_number;
  }

  public String getCvv() {
    return cvv;
  }

  public String getExpirationDate() {
    return expiration_date;
  }
}
