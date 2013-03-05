package com.continuuity.test;

import java.io.IOException;
import java.util.Map;

/**
 * Class for sending queries to running {@link com.continuuity.api.procedure.Procedure Procedure}.
 */
public interface ProcedureClient {

  /**
   * Makes a query to the procedure.
   *
   * @param method Name of the procedure method to query against.
   * @param arguments Arguments to be submitted to the procedure for the query.
   * @return A byte array representing the response.
   * @throws java.io.IOException When there is error querying the procedure.
   */
  byte[] queryRaw(String method, Map<String, String> arguments) throws IOException;


  /**
   * Makes a query to the procedure with {@link String} result.
   *
   * @param method Name of the procedure method to query against.
   * @param arguments Arguments to be submitted to the procedure for the query.
   * @return A {@link String} representing the response.
   * @throws java.io.IOException When there is error querying the procedure.
   */
  String query(String method, Map<String, String> arguments) throws IOException;
}
