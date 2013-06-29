/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * DummyHandlerContext returns an empty runtime arguments.
 */
public class DummyHandlerContext implements HandlerContext {

  @Override
  public Map<String, String> getRunTimeArguments() {
   return ImmutableMap.of();
  }
}
