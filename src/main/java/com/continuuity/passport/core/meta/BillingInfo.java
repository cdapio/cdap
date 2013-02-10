package com.continuuity.passport.core.meta;

/**
*
*/
public class BillingInfo {

  private String creditCardName;
  private String creditCardNumber;
  private String cvv;
  private String expirationDate;

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
