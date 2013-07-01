package com.continuuity.common.utils;

/**
 * Detects type of OS.
 */
public class OSDetector {

  public static boolean isWindows() {
    String os = System.getProperty("os.name").toLowerCase();
    return os.contains("win");
  }
  
  public static boolean isMac() {
    String os = System.getProperty("os.name").toLowerCase();
    return os.contains("mac");
  }
  
  public static boolean isNix() {
    String os = System.getProperty("os.name").toLowerCase();
    return os.contains("nix") || os.contains("nux");
  }
}
