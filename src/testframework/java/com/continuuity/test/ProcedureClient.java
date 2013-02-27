package com.continuuity.test;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public interface ProcedureClient {

  byte[] queryRaw(String method, Map<String, String> arguments) throws IOException;

  String query(String method, Map<String, String> arguments) throws IOException;
}
