package io.cdap.cdap.etl.api.connector;

import co.cask.cdap.api.plugin.connector.ExploreResult;
import io.cdap.cdap.api.plugin.PluginConfigurer;

/**
 * A connector is a plugin which is able to explore and sample an external resource
 */
public interface Connector extends Sampler {

  /**
   * Configure this connector, this method is guaranteed to be called before any other method in this class.
   */
  default void configure(PluginConfigurer configurer) {
    // no-op
  }

  /**
   * Test if the connector is able to connect to the resource
   */
  void test() throws Exception;

  /**
   * Explore the resource on the given path
   */
  ExploreResult explore(ExploreRequest request) throws Exception;
}
