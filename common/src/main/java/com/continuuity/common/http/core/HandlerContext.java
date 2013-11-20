/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.common.http.core;

import java.util.Map;

/**
 * Place holder for information about the environment. Will be passed in during lifecycle management calls of
 * HttpHandlers. Currently has methods to get RunTimeArguments.
 */
public interface HandlerContext {

  /**
   * @return Key Value pairs of runtime arguments.
   */
  Map<String, String> getRuntimeArguments();

  /**
   * @return the {@link HttpResourceHandler} associated with this context,
   * used to let one handler call another internally.
   */
  HttpResourceHandler getHttpResourceHandler();
}
