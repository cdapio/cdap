package com.continuuity.api.procedure;

import java.util.Map;

/**
 *
 */
public interface ProcedureRequest {

  String getMethod();

  Map<String, String> getArguments();

  String getArgument(String key);

  <T> T getArgument(String key, Class<T> type);
}
