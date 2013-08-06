package com.continuuity.app;

import java.util.ResourceBundle;

public final class UserMessages {

  private static final String BUNDLE_NAME = "UserMessages";

  public static String getMessage (String key) {

    return ResourceBundle.getBundle(BUNDLE_NAME).getString(key);

  }

  public static ResourceBundle getBundle () {

    return ResourceBundle.getBundle(BUNDLE_NAME);

  }

}
