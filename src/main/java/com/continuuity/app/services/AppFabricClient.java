/*
* Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
*/
package com.continuuity.app.services;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.internal.app.BufferFileInputStream;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import org.apache.commons.cli.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.jar.JarFile;

/**
 * Client for interacting with local app-fabric service to perform the following operations:
 * a) Deploy locally
 * b) Start/Stop/Status of local service
 * c) promote to cloud
 * <p/>
 * Usage:
 * AppFabricClient client = new AppFabricClient();
 * client.configure(CConfiguration.create(), args);
 * client.execute();
 */
public class AppFabricClient {

  private static Set<String> availableCommands = Sets.newHashSet("deploy", "stop", "start", "help",
    "promote", "status");
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
      transport.open();
      AppFabricService.Client client = new AppFabricService.Client(protocol);

      if ("help".equals(command)) {
        return;
      }

      if ("deploy".equals(command)) {
        deploy(client, transport);
      }

      if ("start".equals(command)) {
        AuthToken dummyAuthToken = new AuthToken("AppFabricClient");
        System.out.println(String.format("Starting application: %s processor %s", application, processor));
        FlowIdentifier identifier = new FlowIdentifier("developer", application, processor, 1);
        RunIdentifier runIdentifier = client.start(dummyAuthToken,
          new FlowDescriptor(identifier, new ArrayList<String>()));

        Preconditions.checkNotNull(runIdentifier, "Problem starting the application");
        System.out.println("Started application with id: " + runIdentifier.getId());
        return;
      }

      if ("stop".equals(command)) {
        AuthToken dummyAuthToken = new AuthToken("AppFabricClient");
        FlowIdentifier identifier = new FlowIdentifier("developer", application, processor, 1);

        RunIdentifier runIdentifier = client.stop(dummyAuthToken, identifier);
        Preconditions.checkNotNull(runIdentifier, "Problem stopping the application");
        System.out.println("Stopped application running with id: " + runIdentifier.getId());
      }

      if ("promote".equals(command)) {
        ResourceIdentifier identifier = new ResourceIdentifier("Developer", this.application, this.resource, 1);
        boolean status = client.promote(new AuthToken(this.authToken), identifier, this.vpc);
        if (status) {
          System.out.println("Promoted to cloud");
        } else {
          System.out.println("Promote to cloud failed");
        }
      }
      if ("status".equals(command)) {
        AuthToken dummyAuthToken = new AuthToken("AppFabricClient");
        FlowIdentifier identifier = new FlowIdentifier("Developer", application, processor, 0);

        FlowStatus flowStatus = client.status(dummyAuthToken, identifier);
        Preconditions.checkNotNull(flowStatus, "Problem getting the status the application");
        System.out.println(String.format("%s", flowStatus.toString()));
      }
    } catch (Exception e) {
      System.out.println(String.format("Caught Exception while running %s ", command));
      System.out.println(String.format("Error: %s", e.getMessage()));
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

    options.addOption(RESOURCE_SHORT_OPT_ARG, RESOURCE_LONG_OPT_ARG, true, "Jar that contains the application.");
    options.addOption(APPLICATION_SHORT_OPT_ARG, APPLICATION_LONG_OPT_ARG, true, "Application Id.");
    options.addOption(PROCESSOR_SHORT_OPT_ARG, PROCESSOR_LONG_OPT_ARG, true, "Processor Id.");
    options.addOption(VPC_SHORT_OPT_ARG, VPC_LONG_OPT_ARG, true, "Fully qualified VPC name to push" +
      " the application to.");
    options.addOption(AUTH_TOKEN_SHORT_OPT_ARG, AUTH_TOKEN_LONG_OPT_ARG, true, "Auth token of the account.");


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
        Preconditions.checkArgument(commandLine.hasOption(APPLICATION_LONG_OPT_ARG), "start command should" +
          " have application argument");
        Preconditions.checkArgument(commandLine.hasOption(PROCESSOR_LONG_OPT_ARG), "start command should " +
          "have processor argument");

        this.application = commandLine.getOptionValue(APPLICATION_LONG_OPT_ARG);
        this.processor = commandLine.getOptionValue(PROCESSOR_LONG_OPT_ARG);
      }
      if ("stop".equals(command)) {
        Preconditions.checkArgument(commandLine.hasOption(APPLICATION_LONG_OPT_ARG), "stop command should have" +
          " application argument");
        Preconditions.checkArgument(commandLine.hasOption(PROCESSOR_LONG_OPT_ARG), "stop command should have " +
          "processor argument");

        this.application = commandLine.getOptionValue(APPLICATION_LONG_OPT_ARG);
        this.processor = commandLine.getOptionValue(PROCESSOR_LONG_OPT_ARG);

      }
      if ("status".equals(command)) {
        Preconditions.checkArgument(commandLine.hasOption(APPLICATION_LONG_OPT_ARG), "status command should have " +
          "application argument");
        Preconditions.checkArgument(commandLine.hasOption(PROCESSOR_LONG_OPT_ARG), "status command should have" +
          " processor argument");

        this.application = commandLine.getOptionValue(APPLICATION_LONG_OPT_ARG);
        this.processor = commandLine.getOptionValue(PROCESSOR_LONG_OPT_ARG);

      }
      if ("promote".equals(command)) {
        Preconditions.checkArgument(commandLine.hasOption(VPC_LONG_OPT_ARG), "promote command should have" +
          "vpc argument");
        Preconditions.checkArgument(commandLine.hasOption(AUTH_TOKEN_LONG_OPT_ARG), "promote command should " +
          "have auth token argument");
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
    String command = "AppFabricClient help|deploy|start|stop|status|promote [OPTIONS]";
    HelpFormatter formatter = new HelpFormatter();
    formatter.setWidth(120);
    formatter.printHelp(command, options);
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

  private void deploy(AppFabricService.Client client, TTransport transport)
    throws IOException, TException, AppFabricServiceException, InterruptedException {
    File file = new File(this.resource);
    JarFile jarFile = new JarFile(file);

    AuthToken dummyAuthToken = new AuthToken("AppFabricClient");
    System.out.println(String.format("Deploying... :%s", this.resource));

    ResourceIdentifier identifier = client.init(dummyAuthToken, new ResourceInfo("developer","", file.getName(),                                                                    (int)file.getTotalSpace(), file.lastModified()));
    Preconditions.checkNotNull(identifier, "Resource identifier is null");

    BufferFileInputStream is =
      new BufferFileInputStream(file.getAbsolutePath(), 100 * 1024);

    try {
      while (true) {
        byte[] toSubmit = is.read();
        if (toSubmit.length == 0) break;
        client.chunk(dummyAuthToken, identifier, ByteBuffer.wrap(toSubmit));
      }
    } finally {
      is.close();
    }
    client.deploy(dummyAuthToken, identifier);

    Thread.sleep(5000);
    DeploymentStatus status = client.dstatus(dummyAuthToken, identifier);

    if (DeployStatus.DEPLOYED.getCode() == status.getOverall()) {
      System.out.println("Deployed");
    } else if (DeployStatus.FAILED.getCode() == status.getOverall()) {
      System.out.println("Deployment failed: ");
    } else {
      System.out.println("Deployment taking more than 5 seconds. Please check the UI for status");
    }
    return;
  }


}
