package com.continuuity.common.http.core;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * DummyHandlerContext.
 */
public class DummyHandlerContext implements HandlerContext {

  @Override
  public Map<String, String> getRunTimeArguments() {
   return ImmutableMap.of();
  }
}
