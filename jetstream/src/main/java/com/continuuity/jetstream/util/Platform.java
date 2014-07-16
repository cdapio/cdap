/*
 * Copyright 2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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

  public static String libraryResource() {
    Platform plat = findPlatform();
    if (plat == X64_LINUX) {
      return "StreamLib_x64_linux.zip";
    } else if (plat == X64_OSX) {
      return "StreamLib_x64_osx.zip";
    } else {
      return null;
    }
  }
}
