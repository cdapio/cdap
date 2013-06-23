/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.core.utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

/**
 * Utility class to generate API KEY.
 * TODO: Use Guava Hasher.
 */
public final class ApiKey {
  /**
   * Generates Random APIKey.
   *
   * @param data inputdata emailId/accountId
   * @return APIKey
   */
  public static String generateKey(String data) throws NoSuchAlgorithmException {

    //Generate random salt and get sha of salt+data
    SecureRandom srandom = new SecureRandom();
    String salt = new BigInteger(176, srandom).toString(10);
    // return DigestUtils.shaHex(salt+data);

    String raw = salt + data;

    byte[] hash = MessageDigest.getInstance("SHA1").digest(raw.getBytes());
    StringBuffer sb = new StringBuffer();
    for (byte b : hash) {
      sb.append(String.format("%02x", b & 0xff));
    }
    return sb.toString();
  }

}
