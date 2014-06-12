/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api;

/**
 * Defines program lifecycle.
 * @param <T> type of the program runtime context
 */
public interface ProgramLifecycle<T extends RuntimeContext> {
  /**
   *  Initializes a Program.
   *  <p>
   *    This method will be called only once per {@link com.continuuity.api.ProgramLifecycle} instance.
   *  </p>
   *  @param context An instance of {@link RuntimeContext}
   *  @throws Exception If there is any error during initialization.
   */
  void initialize(T context) throws Exception;

  /**
   * Destroy is the last thing that gets called before the program is
   * shutdown. So, if there are any cleanups then they can be specified here.
   */
  void destroy();
}
