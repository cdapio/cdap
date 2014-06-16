/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.procedure;

import java.util.Map;

/**
 * This interface defines the request to the {@link Procedure}.
 */
public interface ProcedureRequest {

  /**
   * @return Name of the method
   */
  String getMethod();

  /**
   * @return Arguments passed to the {@link Procedure}
   */
  Map<String, String> getArguments();

  /**
   * Returns an argument value provided by the key argument.
   * @param key of the argument to be retrieved
   * @return If found, returns the value associated with key; if not found, returns null.
   */
  String getArgument(String key);


  /**
   * Returns the value, cast to the <code>type</code> specified.
   * @param key of the argument to be retrieved.
   * @param type of value if found to which it will be cast.
   * @return if found, returns the value cast to <code>type</code>; else null.
   */
  <T> T getArgument(String key, Class<T> type);
}
