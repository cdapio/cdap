package io.cdap.cdap.etl.api.connector;

import co.cask.cdap.api.plugin.connector.ConnectionException;
import co.cask.cdap.api.plugin.connector.EndpointPluginContext;
import co.cask.cdap.api.plugin.connector.ExploreResult;

/**
 * A connector is a plugin which is able to explore and sample an external resource
 */
public interface Connector {
  String PLUGIN_TYPE = "connector";

  /**
   * Test if the connector is able to connect to the resource
   */
  void test(EndpointPluginContext context) throws ConnectionException;

  /**
   * Explore the resource on the given path
   */
  ExploreResult explore(EndpointPluginContext context, String path) throws ConnectionException;


}
