package com.continuuity.common.utils;

import java.util.Arrays;
import java.util.Collection;

/**
 * General string utils.
 */
public class StringUtils {
  public static final  String[] EMPTY_STRING_ARRAY = {};

  /**
   * Splits a comma separated value <code>String</code>, trimming leading and trailing whitespace on each value.
   * @param str a comma separated <String> with values
   * @return a <code>Collection</code> of <code>String</code> values
   */
  public static Collection<String> getTrimmedStringCollection(String str) {
    return Arrays.asList(getTrimmedStrings(str));
  }

  /**
   * Splits a comma separated value <code>String</code>, trimming leading and trailing whitespace on each value.
   * @param str a comma separated <String> with values
   * @return an array of <code>String</code> values
   */
  public static String[] getTrimmedStrings(String str) {
    if (null == str || "".equals(str.trim())) {
      return EMPTY_STRING_ARRAY;
    }
    return str.trim().split("\\s*,\\s*");
  }

}
