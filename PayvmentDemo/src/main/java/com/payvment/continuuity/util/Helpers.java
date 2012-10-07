package com.payvment.continuuity.util;


public class Helpers {

  /**
   * Returns the reverse timestamp of the specified timestamp
   * ({@link Long#MAX_VALUE} - timestamp).
   * @param timestamp
   * @return reverse of specified timestamp
   */
  public static long reverse(long timestamp) {
    return Long.MAX_VALUE - timestamp;
  }

  /**
   * Returns the time-bucketed timestamp of the specified timestamp in
   * milliseconds (timestamp modulo 60*60*1000).
   * @param timestamp stamp in milliseconds
   * @return hour time bucket of specified timestamp in milliseconds
   */
  public static Long hour(Long timestamp) {
    return timestamp % 3600000;
  }

  /**
   * Converts the specified array of strings to an array of byte arrays.
   * @param stringArray array of string
   * @return array of byte arrays
   */
  public static byte [][] saToBa(String [] stringArray) {
    byte [][] byteArrays = new byte[stringArray.length][];
    for (int i=0; i<stringArray.length; i++) {
      byteArrays[i] = Bytes.toBytes(stringArray[i]);
    }
    return byteArrays;
  }
}
