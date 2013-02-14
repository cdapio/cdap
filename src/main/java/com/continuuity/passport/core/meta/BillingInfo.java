package com.continuuity.passport.core.meta;

/**
*
*/
public class BillingInfo {

  private final String creditCardName;
  private final String creditCardNumber;
  private final String cvv;
  private final String expirationDate;

  public BillingInfo(String creditCardName, String creditCardNumber, String cvv, String expirationDate) {
    this.creditCardName = creditCardName;
    this.creditCardNumber = creditCardNumber;
    this.cvv = cvv;
    this.expirationDate = expirationDate;
  }

  public String getCreditCardName() {
    return creditCardName;
  }

  public String getCreditCardNumber() {
    return creditCardNumber;
  }

  public String getCvv() {
    return cvv;
  }

  public String getExpirationDate() {
    return expirationDate;
  }
}
