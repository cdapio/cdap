package com.continuuity.gateway.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.gateway.Gateway;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Main is a simple class that allows us to launch the Gateway as a standalone
 * program. This is also where we do our runtime injection.
 */
public class NoopGateway {

  /**
   * Our main method.
   *
   * @param args Our command line options
   */
  public static void main(String[] args) {

    // Set up our Guice injections
    Injector injector = Guice.createInjector(
      new GatewayModules().getNoopModules(),
      new DataFabricModules().getNoopModules());

    // Get our fully wired Gateway
    Gateway theGateway = injector.getInstance(Gateway.class);

    // Now, initialize the Gateway
    try {

      // Load our configuration from our resource files
      CConfiguration configuration = CConfiguration.create();

      // Start the gateway!
      theGateway.start(null, configuration);

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }

    try {
      Thread.sleep(1000000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

} // End of Main

