/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.core.utils;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.math.BigInteger;
import java.security.SecureRandom;

/**
 *
 */
public class NonceUtils {

  private static final HashFunction hashFunction = Hashing.sha1();

  public static int getNonce() {
    SecureRandom srandom = new SecureRandom();
    BigInteger bigInt = new BigInteger(176, srandom);
    return (Math.abs(bigInt.intValue()));
  }

  public static int getNonce(String id) {
    return hashFunction.newHasher().putLong(System.currentTimeMillis()).putString(id).hashCode();
  }


}
