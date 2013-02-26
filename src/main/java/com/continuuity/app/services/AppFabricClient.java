/*
* Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
*/
package com.continuuity.app.services;

import com.continuuity.app.Id;
import com.continuuity.app.deploy.Manager;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.guice.BigMamaModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.cli.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

/**
 * Client for interacting with local app-fabric service to perform the following operations:
 * a) Deploy locally
 * b) Verify jar
 * c) Start/Stop/Status of local service
 * d) promote to cloud
 * <p/>
 * Usage:
 * AppFabricClient client = new AppFabricClient();
 * client.configure(CConfiguration.create(), args);
 * client.execute();
 */
public class AppFabricClient {

  private static Set<String> availableCommands = Sets.newHashSet("deploy", "stop", "start", "help",
                                                                 "promote", "verify", "status");
  private final String RESOURCE_LONG_OPT_ARG = "resource";
  private final String APPLICATION_LONG_OPT_ARG = "application";
  private final String PROCESSOR_LONG_OPT_ARG = "processor";
  private final String VPC_LONG_OPT_ARG = "vpc";
  private final String AUTH_TOKEN_LONG_OPT_ARG = "authtoken";


  private final String RESOURCE_SHORT_OPT_ARG = "r";
  private final String APPLICATION_SHORT_OPT_ARG = "a";
  private final String PROCESSOR_SHORT_OPT_ARG = "p";
  private final String VPC_SHORT_OPT_ARG = "v";
  private final String AUTH_TOKEN_SHORT_OPT_ARG = "t";

  private String resource = null;
  private String application = null;
  private String processor = null;
  private String vpc = null;
  private String authToken = null;

  private String command = null;

  public String getCommand() {
    return command;
  }

  private CConfiguration configuration;
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricClient.class);

  /**
   * Execute the configured operation
   */
  public void execute() throws TException, AppFabricServiceException {
    Preconditions.checkNotNull(command, "App client is not configured to run");
    Preconditions.checkNotNull(configuration, "App client configuration is not set");

    String address = "localhost";
    int port = configuration.getInt(Constants.CFG_APP_FABRIC_SERVER_PORT, Constants.DEFAULT_APP_FABRIC_SERVER_PORT);
    TTransport transport = null;
    TProtocol protocol = null;
    try {
      transport = new TFramedTransport(new TSocket(address, port));
      protocol = new TBinaryProtocol(transport);
      AppFabricService.Client client = new AppFabricService.Client(protocol);

      if ("help".equals(command)) {
        return;
      }
      if ("deploy".equals(command)) {
        AuthToken dummyAuthToken = new AuthToken("AppFabricClient");
        ResourceIdentifier resourceIdentifier = new ResourceIdentifier("Account", "Application", this.resource, 0);
        client.deploy(dummyAuthToken, resourceIdentifier);
        LOG.info("Deployed: " + resource);
        return;
      }

      if ("start".equals(command)) {
        AuthToken dummyAuthToken = new AuthToken("AppFabricClient");
        FlowIdentifier identifier = new FlowIdentifier("Account", application, processor, 0);
        RunIdentifier runIdentifier = client.start(dummyAuthToken,
                                                   new FlowDescriptor(identifier, new ArrayList<String>()));

        Preconditions.checkNotNull(runIdentifier, "Problem starting the application");
        LOG.info("Started application with id: " + runIdentifier.getId());
        return;
      }

      if ("stop".equals(command)) {
        AuthToken dummyAuthToken = new AuthToken("AppFabricClient");
        FlowIdentifier identifier = new FlowIdentifier("Account", application, processor, 0);

        RunIdentifier runIdentifier = client.stop(dummyAuthToken, identifier);
        Preconditions.checkNotNull(runIdentifier, "Problem stopping the application");
        LOG.info("Stopped application running with id: " + runIdentifier.getId());
      }

      if ("promote".equals(command)) {
        ResourceIdentifier identifier = new ResourceIdentifier("Account", this.application, this.resource, 0);
        boolean status = client.promote(new AuthToken(this.authToken),identifier, this.vpc);
        if (status) {
          LOG.info("Promoted to cloud");
        }
        else {
          LOG.info("Promote to cloud failed");
        }
      }
      if ("status".equals(command)) {
        AuthToken dummyAuthToken = new AuthToken("AppFabricClient");
        FlowIdentifier identifier = new FlowIdentifier("Account", application, processor, 0);

        FlowStatus flowStatus = client.status(dummyAuthToken, identifier);
        Preconditions.checkNotNull(flowStatus, "Problem getting the status the application");
        LOG.info(flowStatus.toString());
      }

      if ("verify".equals(command)) {
        Location location = new LocalLocationFactory().create(this.resource);
        final Injector injector = Guice.createInjector(new BigMamaModule(configuration),
                                                       new DataFabricModules().getInMemoryModules());

        ManagerFactory factory = injector.getInstance(ManagerFactory.class);
        Manager<Location, ApplicationWithPrograms> manager = (Manager<Location, ApplicationWithPrograms>) factory.create();
        manager.deploy(new Id.Account("Account"), location);
        LOG.info("Verification succeeded");

      }

    } catch (Exception e) {
      LOG.info("Caught Exception while verifying application");
      throw Throwables.propagate(e);
    } finally {
      transport.close();
    }
  }

  /**
   * Configure the Client to execute commands
   *
   * @param configuration Instance of {@code CConfiguration}
   * @param args          array of String arguments
   * @return Command that will be executed
   * @throws ParseException on errors in commnd line parsing
   */
  public String configure(CConfiguration configuration, String args[]) {

    this.configuration = configuration;

    Preconditions.checkArgument(args.length >= 1, "Not enough arguments");
    boolean knownCommand = availableCommands.contains(args[0]);
    Preconditions.checkArgument(knownCommand, "Unknown Command specified");

    command = args[0];

    CommandLineParser commandLineParser = new GnuParser();

    Options options = new Options();

    options.addOption(RESOURCE_SHORT_OPT_ARG,RESOURCE_LONG_OPT_ARG, true, "Jar that contains the application.");
    options.addOption(APPLICATION_SHORT_OPT_ARG,APPLICATION_LONG_OPT_ARG, true, "Application Id.");
    options.addOption(PROCESSOR_SHORT_OPT_ARG,PROCESSOR_LONG_OPT_ARG, true, "Processor Id.");
    options.addOption(VPC_SHORT_OPT_ARG,VPC_LONG_OPT_ARG, true, "Fully qualified VPC name to push the application to.");
    options.addOption(AUTH_TOKEN_SHORT_OPT_ARG,AUTH_TOKEN_LONG_OPT_ARG, true, "Auth token of the account.");


    CommandLine commandLine = null;

    try {
      commandLine = commandLineParser.parse(options, Arrays.copyOfRange(args, 1, args.length));

      if ("help".equals(command)) {
        printHelp(options);
      }
      //Check if the appropriate args are passed in for each of the commands

      if ("deploy".equals(command)) {
        Preconditions.checkArgument(commandLine.hasOption(RESOURCE_LONG_OPT_ARG),
                                    "deploy command should have resource argument");
        this.resource = commandLine.getOptionValue(RESOURCE_LONG_OPT_ARG);
      }
      if ("start".equals(command)) {
        Preconditions.checkArgument(commandLine.hasOption(APPLICATION_LONG_OPT_ARG), "start command should have application argument");
        Preconditions.checkArgument(commandLine.hasOption(PROCESSOR_LONG_OPT_ARG), "start command should have processor argument");

        this.application = commandLine.getOptionValue(APPLICATION_LONG_OPT_ARG);
        this.processor = commandLine.getOptionValue(PROCESSOR_LONG_OPT_ARG);
      }
      if ("stop".equals(command)) {
        Preconditions.checkArgument(commandLine.hasOption(APPLICATION_LONG_OPT_ARG), "stop command should have application argument");
        Preconditions.checkArgument(commandLine.hasOption(PROCESSOR_LONG_OPT_ARG), "stop command should have processor argument");

        this.application = commandLine.getOptionValue(APPLICATION_LONG_OPT_ARG);
        this.processor = commandLine.getOptionValue(PROCESSOR_LONG_OPT_ARG);

      }
      if ("status".equals(command)) {
        Preconditions.checkArgument(commandLine.hasOption(APPLICATION_LONG_OPT_ARG), "status command should have " +
          "application argument");
        Preconditions.checkArgument(commandLine.hasOption(PROCESSOR_LONG_OPT_ARG), "status command should have processor argument");

        this.application = commandLine.getOptionValue(APPLICATION_LONG_OPT_ARG);
        this.processor = commandLine.getOptionValue(PROCESSOR_LONG_OPT_ARG);

      }
      if ("verify".equals(command)) {
        Preconditions.checkArgument(commandLine.hasOption(RESOURCE_LONG_OPT_ARG), "verify command should have resource argument");
        this.resource = commandLine.getOptionValue(RESOURCE_LONG_OPT_ARG);
      }
      if ("promote".equals(command)) {
        Preconditions.checkArgument(commandLine.hasOption(VPC_LONG_OPT_ARG), "promote command should have vpc argument");
        Preconditions.checkArgument(commandLine.hasOption(AUTH_TOKEN_LONG_OPT_ARG), "promote command should have auth token argument");
        Preconditions.checkArgument(commandLine.hasOption(APPLICATION_LONG_OPT_ARG), "promote command should have" +
          " application argument");

        this.vpc = commandLine.getOptionValue(VPC_LONG_OPT_ARG);
        this.authToken = commandLine.getOptionValue(AUTH_TOKEN_LONG_OPT_ARG);
        this.application = commandLine.getOptionValue(APPLICATION_LONG_OPT_ARG);
      }
    } catch (ParseException e) {
      printHelp(options);
    } catch (Exception e) {
      printHelp(options);
      throw Throwables.propagate(e);
    }

    return command;
  }

  private void printHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("AppFabricClient help|deploy|start|stop|status|verify|promote [OPTIONS]", options);
  }

  public static void main(String[] args) throws TException, AppFabricServiceException {
    String command = null;
    AppFabricClient client = null;
    try {
      client = new AppFabricClient();
      client.configure(CConfiguration.create(), args);
    } catch (Exception e) {
      return;
    }
    client.execute();
  }


}
