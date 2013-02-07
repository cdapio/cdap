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
   * Returns an argument value provided an key for argument
   * @param key of the argument to be retrieved
   * @return if found, returns the value associated with key; if not found, returns null.
   */
  String getArgument(String key);


  /**
   * Returns the value casted to the <code>type</code> specified.
   * @param key of the argument to be retrieved.
   * @param type of value if found to be casted to.
   * @return if found, returns the value casted to <code>type</code>; else null.
   */
  <T> T getArgument(String key, Class<T> type);
}
