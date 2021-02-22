package io.cdap.cdap.etl.api.connector;

/**
 * Context to retrieve connection, providing access to load other plugins if needed
 */
public interface ConnectionContext {

  /**
   * Get the connection in the system scope
   */
  Connection getConnection(String connectionName) throws Exception;

  /**
   * Get a specific connection in a namespace
   */
  Connection getConnection(String namespace, String connectionName) throws Exception;
}
