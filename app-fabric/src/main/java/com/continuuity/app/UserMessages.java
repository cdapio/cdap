package com.continuuity.app;

import java.util.MissingResourceException;
import java.util.ResourceBundle;

public final class UserMessages {

  private static final String BUNDLE_NAME = "UserMessages";

  public static String getMessage (String key) {

    try {
      return ResourceBundle.getBundle(BUNDLE_NAME).getString(key);

    } catch (NullPointerException e) {
      return "Unknown Error. Please check the Reactor log.";

    } catch (MissingResourceException e) {
      return "Unknown Error. Please check the Reactor log.";

    } catch (ClassCastException e) {
      return "Unknown Error. Please check the Reactor log.";
    }

  }

  public static ResourceBundle getBundle () {

    return ResourceBundle.getBundle(BUNDLE_NAME);

  }

}
