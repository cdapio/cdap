package com.continuuity.app.runtime;

import java.util.Map;

/**
 *
 */
public interface Arguments extends Iterable<Map.Entry<String, String>> {

  boolean hasOption(String optionName);

  /**
   * Returns option value for the given option name.
   *
   * @param name Name of the option.
   * @return The value associated with the given name or {@code null} if no such option exists.
   */
  String getOption(String name);

  String getOption(String name, String defaultOption);
}
