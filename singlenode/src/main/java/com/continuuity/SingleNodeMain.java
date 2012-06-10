/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.flow.manager.server.FAR.FARServer;
import com.continuuity.flow.manager.server.FlowManager.FlowManagerServer;
import com.continuuity.gateway.runtime.GatewayProductionModule;
import com.continuuity.data.runtime.DataFabricInMemoryModule;
import com.continuuity.flow.runtime.FARRuntime;
import com.continuuity.flow.runtime.FlowManagerRuntime;
import com.continuuity.gateway.Gateway;

import com.continuuity.metrics.service.MetricsServer;
import com.continuuity.runtime.MetricsRuntime;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
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
  private MetricsServer theOverlord;

  /**
   * This is the FAR server.
   */
  @Inject
  private FARServer theFARServer;

  /**
   * This is the FlowManager server
   */
  @Inject
  private FlowManagerServer theFlowManager;

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

    System.out.println("==============================================================" +
      "==============");
    System.out.println(" Continuuity BigFlow - Copyright 2012 Continuuity, Inc. All " +
      "Rights Reserved.");
    System.out.println("");

    //if (zookeeper != null) {
      try {

        System.out.println(" Starting Zookeeper Service");
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

        System.out.println(" Starting Gateway Service");
        theGateway.configure(myConfiguration);
        theGateway.start();

      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      throw new IllegalStateException(
        "Unable to start, Gateway service is null");
    }

    if (theFlowManager != null) {
      try {

        System.out.println(" Starting FlowManager Service");
        theFlowManager.start(null, myConfiguration);

      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      throw new IllegalStateException(
        "Unable to start, FlowManager service is null");
    }

    if (theOverlord != null) {
      try {

        System.out.println(" Starting Metrics Service");
        theOverlord.start(null, myConfiguration);

      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      throw new IllegalStateException(
        "Unable to start, Metrics service is null");
    }

    // TODO: Also, need to start web-cloud-app



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
    FARRuntime farRuntime = new FARRuntime();
    FlowManagerRuntime flowManagerRuntime = new FlowManagerRuntime();
    MetricsRuntime metricsRuntime = new MetricsRuntime();


    // Set up our Guice injections
    Injector injector = Guice.createInjector(
        farRuntime.getSingleNode(),
        flowManagerRuntime.getSingleNode(),
        metricsRuntime.getSingleNode(),
        new GatewayProductionModule(),
        new DataFabricInMemoryModule()
    );

    // Create our server instance
    SingleNodeMain continuuity = injector.getInstance(SingleNodeMain.class);

    // Load all our config files
    continuuity.loadConfiguration();

    // Now bootstrap all of the services
    continuuity.bootStrapServices();

  }

} // end of SingleNodeMain class
