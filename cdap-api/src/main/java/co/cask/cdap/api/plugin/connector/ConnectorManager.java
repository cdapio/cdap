package co.cask.cdap.api.plugin.connector;

import java.util.List;

/**
 * Provides access to connector information
 */
public interface ConnectorManager {

  /**
   * List the available connections in a namespace
   */
  List<Connection> list(String namespace);

  /**
   * Get a specific connection in a namespace
   *
   * @throws ConnectionException if there is an error fetching the connection
   */
  Connection getConnection(String namespace, String connectionName) throws ConnectionException;

  /**
   * Get the specific sample in a namespace
   */
  Sample getSample(String namespace, String sampleId) throws ConnectionException;
}
