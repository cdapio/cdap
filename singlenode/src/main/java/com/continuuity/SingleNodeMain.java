/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricInMemoryModule;
import com.continuuity.flow.manager.server.FlowManager;
import com.continuuity.flow.runtime.FlowSingleNodeModule;
import com.continuuity.gateway.Gateway;
import com.continuuity.gateway.runtime.GatewayProductionModule;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * SingleNodeMain is the master main method for the Continuuity single node
 * platform. This is where we load all our external configuration and bootstrap
 * all the services that comprise the platform.
 */
public class SingleNodeMain {

  /**
   * This is our Logger instance
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(SingleNodeMain.class);

  /**
   * This is the data fabric service
   */
  @Inject
  private OperationExecutor theFabric;

  /**
   * This is the Zookeeper service.
   *
   * TODO: Find somewhere to create this so we can inject it
   */
  private InMemoryZookeeper zookeeper;

  /**
   * This is the Gateway service
   */
  @Inject
  private Gateway theGateway;

  /**
   * This is the Metrics Monitor service
   */
  @Inject
  // TODO: Make this the real class
  private Object theOverlord;

  /**
   * This is the FlowManager service
   */
  @Inject
  private FlowManager theFlowManager;

  /**
   * This is our universal configurations object.
   */
  private CConfiguration myConfiguration;


  /**
   * Bootstrap is where we initialize all the services that make up the
   * SingleNode version.
   *
   * TODO: Create a "service" interface that all our top level services can
   * implement. We can then clean up the code below and generify it.
   */
  private void bootStrapServices() {

    LOG.info("==============================================================" +
      "==============");
    LOG.info(" Continuuity BigFlow - Copyright 2012 Continuuity, Inc. All " +
      "Rights Reserved.");
    LOG.info("");

    //if (zookeeper != null) {
      try {

        LOG.info(" Starting Zookeeper Service");
        zookeeper =
          new InMemoryZookeeper(
            Integer.parseInt(myConfiguration.get("zookeeper.port")),
            new File(myConfiguration.get("zookeeper.datadir")) );

      } catch (Exception e) {
        e.printStackTrace();
      }
    /*} else {
      throw new IllegalStateException(
        "Unable to start, Zookeeper service is null");
    }   */

    if (theGateway != null) {
      try {

        LOG.info(" Starting Gateway Service");
        theGateway.start();

      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      throw new IllegalStateException(
        "Unable to start, Gateway service is null");
    }

    if (theOverlord != null) {
      try {

        LOG.info(" Starting Metrics Service");
        // TODO: Really start it

      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      throw new IllegalStateException(
        "Unable to start, Metrics service is null");
    }

    if (theFlowManager != null) {
      try {

        LOG.info(" Starting FlowManager Service");
        theFlowManager.start(myConfiguration);

      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      throw new IllegalStateException(
        "Unable to start, Metrics service is null");
    }



  } // end of bootStrapServices


  /**
   * Load Configuration looks for all of the config xml files in the resources
   * directory, and loads all of the properties into those files.
   */
  private void loadConfiguration() {

    // Create our config object
    myConfiguration = CConfiguration.create();

    // Clear all of the hadoop settings
    myConfiguration.clear();

    // TODO: Make this generic and scan for files before adding them
    myConfiguration.addResource("continuuity-flowmanager.xml");
    myConfiguration.addResource("continuuity-gateway.xml");

  } // end of loadConfiguration


  /**
   * The root of all goodness!
   *
   * @param args Our cmdline arguments
   */
  public static void main(String[] args) {

    // We don't support command line options currently

    // Set up our Guice injections
    Injector injector = Guice.createInjector(
        new GatewayProductionModule(),
        new DataFabricInMemoryModule(),
        new FlowSingleNodeModule() );

    // Create our server instance
    SingleNodeMain continuuity = injector.getInstance(SingleNodeMain.class);

    // Load all our config files
    continuuity.loadConfiguration();

    // Now bootstrap all of the services
    continuuity.bootStrapServices();

  }

} // end of SingleNodeMain class
