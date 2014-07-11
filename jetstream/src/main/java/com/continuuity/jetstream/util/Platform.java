package com.continuuity.jetstream.util;

/**
 *
 */
public enum Platform {
  X64_OSX,
  X64_LINUX,
  X64_WINDOWS,
  UNSUPPORTED;

  private static Platform platform = null;

  public static Platform findPlatform() {
    String os = System.getProperty("os.name").toLowerCase();
    String arch = System.getProperty("os.arch").toLowerCase();
    if (platform != null) {
      return platform;
    }

    if (arch.endsWith("64")) {
      if (os.startsWith("win")) {
        platform = X64_WINDOWS;
      } else if (os.endsWith("nux")) {
        platform = X64_LINUX;
      } else if (os.startsWith("mac")) {
        platform = X64_OSX;
      } else {
        platform = UNSUPPORTED;
      }
    } else {
      platform = UNSUPPORTED;
    }

    return platform;
  }

  public static String gsLibraryResource() {
    Platform plat = findPlatform();
    if (plat == X64_LINUX) {
      return "GSStreamLib_x64_linux.zip";
    } else if (plat == X64_OSX) {
      return "GSStreamLib_x64_osx.zip";
    } else {
      return null;
    }
  }
}
