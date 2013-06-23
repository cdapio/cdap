package com.continuuity.passport.core.utils;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 *
 */
public class PasswordUtils {
  private static final HashFunction hashFunction = Hashing.sha1();

  public static String generateHashedPassword(String password) {
    HashCode hashCode = hashFunction.newHasher().putString(password).hash();
    return hashCode.toString();
  }

}
