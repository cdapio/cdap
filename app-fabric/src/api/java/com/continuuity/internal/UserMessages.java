package com.continuuity.internal;

import java.util.ResourceBundle;

/**
 * Helper class for getting error messages.
 */
public final class UserMessages {

  private static final String BUNDLE_NAME = "UserMessages";

  public static String getMessage (String key) {

    try {
      return getBundle().getString(key);

    } catch (Exception e) {
      return "Unknown Error. Please check the Reactor log.";
    }

  }

  public static ResourceBundle getBundle () {

    return ResourceBundle.getBundle(BUNDLE_NAME);

  }

}
