package com.continuuity.passport.core.utils;

import java.math.BigInteger;
import java.security.SecureRandom;

/**
 *
 */
public class NonceUtils {

  public static int getNonce() {
    SecureRandom srandom = new SecureRandom();
    BigInteger bigInt = new BigInteger(176, srandom);
    return (Math.abs(bigInt.intValue()));
  }

}
