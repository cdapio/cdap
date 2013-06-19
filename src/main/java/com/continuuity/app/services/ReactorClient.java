/*
* Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
*/
package com.continuuity.app.services;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.Copyright;
import com.continuuity.internal.app.BufferFileInputStream;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

/**
 * Client for interacting with local app-fabric service to perform the following operations:
 * a) Deploy locally
 * b) Start/Stop/Status of local service
 * c) Promote to cloud
 * d) Change the number of flowlet instances
 * <p/>
 * Usage:
 * ReactorClient client = new ReactorClient();
 * client.configure(CConfiguration.create(), args);
 * client.execute();
 */

public class ReactorClient {

  private static final String DEVELOPER_ACCOUNT_ID = "developer";
  private static final Set<String> AVAILABLE_COMMANDS = Sets.newHashSet("deploy", "stop", "start", "help", "promote",
                                                                        "status", "scale");
  private static final String ARCHIVE_LONG_OPT_ARG = "archive";
  private static final String APPLICATION_LONG_OPT_ARG = "application";
  private static final String PROCEDURE_LONG_OPT_ARG = "procedure";
  private static final String FLOW_LONG_OPT_ARG = "flow";
  private static final String FLOWLET_LONG_OPT_ARG = "flowlet";
  private static final String FLOWLET_INSTANCES_LONG_OPT_ARG = "instances";
  private static final String MAPREDUCE_LONG_OPT_ARG = "mapreduce";
  private static final String HOSTNAME_LONG_OPT_ARG = "host";
  private static final String APIKEY_LONG_OPT_ARG = "apikey";

  private String resource = null;
  private String application = null;
  private String procedure = null;
  private String flow = null;
  private String flowlet = null;
  private short flowletInstances = 1;
  private String mapReduce = null;
  private String hostname = null;
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
    TProtocol protocol;
    try {
      transport = new TFramedTransport(new TSocket(address, port));
      protocol = new TBinaryProtocol(transport);
      transport.open();
      AppFabricService.Client client = new AppFabricService.Client(protocol);

      if ("help".equals(command)) {
        return;
      }

      if ("deploy".equals(command)) {
        deploy(client);
      }

      if ("start".equals(command)) {
        AuthToken dummyAuthToken = new AuthToken("ReactorClient");
        FlowIdentifier identifier;
        if( this.flow != null) {
          identifier = new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, this.flow, 1);
          identifier.setType(EntityType.FLOW);
          System.out.println(String.format("Starting flow %s for application %s ", this.flow, this.application));
        } else if (this.mapReduce != null) {
          identifier = new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, this.mapReduce, 1);
          identifier.setType(EntityType.MAPREDUCE);
          System.out.println(String.format("Starting mapreduce job %s for application %s ",
                                           this.mapReduce, this.application));
        } else {
          identifier = new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, this.procedure, 1);
          identifier.setType(EntityType.QUERY);
          System.out.println(String.format("Starting procedure %s for application %s ",
                                           this.procedure, this.application));
        }
        client.start(dummyAuthToken, new FlowDescriptor(identifier, ImmutableMap.<String, String>of()));
        System.out.println("Started ");
        return;
      }

      if ("stop".equals(command)) {

        AuthToken dummyAuthToken = new AuthToken("ReactorClient");
        FlowIdentifier identifier;
        if( this.flow != null) {
          identifier = new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, this.flow, 1);
          identifier.setType(EntityType.FLOW);
          System.out.println(String.format("Stopping flow %s for application %s ", this.flow, this.application));
        } else if (this.mapReduce != null) {
          identifier = new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, this.mapReduce, 1);
          identifier.setType(EntityType.MAPREDUCE);
          System.out.println(String.format("Killing mapreduce job %s for application %s ",
                                           this.mapReduce, this.application));
        } else {
          identifier = new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, this.procedure, 1);
          identifier.setType(EntityType.QUERY);
          System.out.println(String.format("Stopping procedure %s for application %s ",
                                           this.procedure, this.application));
        }
        client.stop(dummyAuthToken, identifier);
        System.out.println("Stopped ");
        return;
      }

      if ("scale".equals(command)) {
        AuthToken dummyAuthToken = new AuthToken("ReactorClient");
        FlowIdentifier identifier = null;
        if( this.flow != null) {
          identifier = new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, this.flow, 1);
          identifier.setType(EntityType.FLOW);
          System.out.println(String.format(
            "Changing number of flowlet instances for flowlet %s in flow %s of application %s ",flowlet, flow,
            application));
        }
        client.setInstances(dummyAuthToken, identifier, flowlet, flowletInstances);
        System.out.println("The number of flowlet instances has been changed.");
        return;
      }

      if ("promote".equals(command)) {
        ResourceIdentifier identifier = new ResourceIdentifier(DEVELOPER_ACCOUNT_ID, this.application, "noresource", 1);
        boolean status = client.promote(new AuthToken(this.authToken), identifier, this.hostname);
        if (status) {
          System.out.println("Promoted to cloud");
        } else {
          System.out.println("Promote to cloud failed");
        }
      }
      if ("status".equals(command)) {
        AuthToken dummyAuthToken = new AuthToken("ReactorClient");
        FlowIdentifier identifier;
        if( this.flow != null) {
          identifier = new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, this.flow, 1);
          identifier.setType(EntityType.FLOW);
          System.out.println(String.format("Getting status for flow %s in application %s ",
                                           this.flow, this.application));
        }
        else {
          identifier = new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, this.procedure, 1);
          identifier.setType(EntityType.QUERY);
          System.out.println(String.format("Getting status for procedure %s in application %s ",
                                           this.flow, this.application));
        }
        FlowStatus flowStatus = client.status(dummyAuthToken, identifier);
        Preconditions.checkNotNull(flowStatus, "Problem getting the status the application");
        System.out.println(String.format("Status: %s", flowStatus.toString()));
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
   */
  public String configure(CConfiguration configuration, String args[]) {

    this.configuration = configuration;
    this.command = null;
    Preconditions.checkArgument(args.length >= 1, "Not enough arguments");
    boolean knownCommand = AVAILABLE_COMMANDS.contains(args[0]);
    Preconditions.checkArgument(knownCommand, "Unknown Command specified");

    String sentCommand = args[0];

    CommandLineParser commandLineParser = new GnuParser();

    Options options = new Options();

    options.addOption("a", APPLICATION_LONG_OPT_ARG, true, "Application Id.");
    options.addOption("r", ARCHIVE_LONG_OPT_ARG, true, "Archive that contains the application.");
    options.addOption("p", PROCEDURE_LONG_OPT_ARG, true, "Procedure Id.");
    options.addOption("h", HOSTNAME_LONG_OPT_ARG, true, "Hostname to push the application to.");
    options.addOption("k", APIKEY_LONG_OPT_ARG, true, "Apikey of the account.");
    options.addOption("f", FLOW_LONG_OPT_ARG, true, "Flow Id.");
    options.addOption("l", FLOWLET_LONG_OPT_ARG, true, "Flowlet Id.");
    options.addOption("i", FLOWLET_INSTANCES_LONG_OPT_ARG, true, "Flowlet Instances.");
    options.addOption("m", MAPREDUCE_LONG_OPT_ARG, true, "MapReduce job Id.");

    CommandLine commandLine;

    try {
      commandLine = commandLineParser.parse(options, Arrays.copyOfRange(args, 1, args.length));

      if ("help".equals(sentCommand)) {
        printHelp();
      }

      //Check if the appropriate args are passed in for each of the commands
      if ("deploy".equals(sentCommand)) {
        Preconditions.checkArgument(commandLine.hasOption(ARCHIVE_LONG_OPT_ARG),
                                    "deploy command should have archive argument");
        this.resource = commandLine.getOptionValue(ARCHIVE_LONG_OPT_ARG);
      }
      if ("start".equals(sentCommand)) {
        Preconditions.checkArgument(commandLine.hasOption(APPLICATION_LONG_OPT_ARG), "status command should have " +
          "application argument");
        Preconditions.checkArgument(commandLine.hasOption(PROCEDURE_LONG_OPT_ARG) ||
                                      commandLine.hasOption(FLOW_LONG_OPT_ARG) ||
                                      commandLine.hasOption(MAPREDUCE_LONG_OPT_ARG),
                                    "start command should have procedure or flow or mapreduce argument");

        this.application = commandLine.getOptionValue(APPLICATION_LONG_OPT_ARG);
        this.procedure = commandLine.getOptionValue(PROCEDURE_LONG_OPT_ARG);
        this.flow = commandLine.getOptionValue(FLOW_LONG_OPT_ARG);
        this.mapReduce = commandLine.getOptionValue(MAPREDUCE_LONG_OPT_ARG);
      }
      if ("stop".equals(sentCommand)) {
        Preconditions.checkArgument(commandLine.hasOption(APPLICATION_LONG_OPT_ARG), "status command should have " +
          "application argument");
        Preconditions.checkArgument(commandLine.hasOption(PROCEDURE_LONG_OPT_ARG) ||
                                      commandLine.hasOption(FLOW_LONG_OPT_ARG) ||
                                      commandLine.hasOption(MAPREDUCE_LONG_OPT_ARG),
                                    "stop command should have procedure or flow or mapreduce argument");

        this.application = commandLine.getOptionValue(APPLICATION_LONG_OPT_ARG);
        this.procedure = commandLine.getOptionValue(PROCEDURE_LONG_OPT_ARG);
        this.flow = commandLine.getOptionValue(FLOW_LONG_OPT_ARG);
        this.mapReduce = commandLine.getOptionValue(MAPREDUCE_LONG_OPT_ARG);

      }
      if ("status".equals(sentCommand)) {
        Preconditions.checkArgument(commandLine.hasOption(APPLICATION_LONG_OPT_ARG), "status command should have " +
          "application argument");
        Preconditions.checkArgument(commandLine.hasOption(PROCEDURE_LONG_OPT_ARG) ||
                                      commandLine.hasOption(FLOW_LONG_OPT_ARG) ||
                                      commandLine.hasOption(MAPREDUCE_LONG_OPT_ARG),
                                    "status command should have procedure or flow or mapreduce argument");

        this.application = commandLine.getOptionValue(APPLICATION_LONG_OPT_ARG);
        this.procedure = commandLine.getOptionValue(PROCEDURE_LONG_OPT_ARG);
        this.flow = commandLine.getOptionValue(FLOW_LONG_OPT_ARG);
        this.mapReduce = commandLine.getOptionValue(MAPREDUCE_LONG_OPT_ARG);

      }
      if ("scale".equals(sentCommand)) {
        Preconditions.checkArgument(commandLine.hasOption(APPLICATION_LONG_OPT_ARG), "status command should have " +
          "application argument");
        Preconditions.checkArgument(commandLine.hasOption(FLOW_LONG_OPT_ARG),
                                    "status command should have flow argument");
        Preconditions.checkArgument(commandLine.hasOption(FLOWLET_LONG_OPT_ARG),
                                    "status command should have flowlet argument");
        Preconditions.checkArgument(commandLine.hasOption(FLOWLET_INSTANCES_LONG_OPT_ARG),
                                    "status command should have number of flowlet instances argument");

        this.application = commandLine.getOptionValue(APPLICATION_LONG_OPT_ARG);
        this.flow = commandLine.getOptionValue(FLOW_LONG_OPT_ARG);
        this.flowlet = commandLine.getOptionValue(FLOWLET_LONG_OPT_ARG);

        this.flowletInstances = Short.parseShort(commandLine.getOptionValue(FLOWLET_INSTANCES_LONG_OPT_ARG));
        Preconditions.checkArgument(flowletInstances > 0);
      }
      if ("promote".equals(sentCommand)) {
        Preconditions.checkArgument(commandLine.hasOption(HOSTNAME_LONG_OPT_ARG), "promote command should have" +
          "vpc argument");
        Preconditions.checkArgument(commandLine.hasOption(APIKEY_LONG_OPT_ARG), "promote command should " +
          "have auth token argument");
        Preconditions.checkArgument(commandLine.hasOption(APPLICATION_LONG_OPT_ARG), "promote command should have" +
          " application argument");

        this.hostname = commandLine.getOptionValue(HOSTNAME_LONG_OPT_ARG);
        this.authToken = commandLine.getOptionValue(APIKEY_LONG_OPT_ARG);
        this.application = commandLine.getOptionValue(APPLICATION_LONG_OPT_ARG);
      }

      command = sentCommand;

    } catch (ParseException e) {
      printHelp();
    } catch (Exception e) {
      printHelp();
      throw Throwables.propagate(e);
    }

    return command;
  }

  private void printHelp() {
    PrintStream out = System.out;
    Copyright.print(out);

    out.println("Usage:");
    out.println("  reactor-client deploy    --archive <filename>");
    out.println("  reactor-client start     --application <id> ( --flow <id> | --procedure <id> | --mapreduce <id>)");
    out.println("  reactor-client stop      --application <id> ( --flow <id> | --procedure <id> | --mapreduce <id>)");
    out.println("  reactor-client status    --application <id> ( --flow <id> | --procedure <id> | --mapreduce <id>)");
    out.println("  reactor-client scale     --application <id> --flow <id> --flowlet <id> --instances <number>");
    out.println("  reactor-client promote   --application <id> --host <hostname> --apikey <key>");

    out.println("Options:");
    out.println("  --archive <filename> \t Archive containing the application.");
    out.println("  --application <id> \t Application Id.");
    out.println("  --flow <id> \t\t Flow id of the application.");
    out.println("  --procedure <id> \t Procedure of in the application.");
    out.println("  --mapreduce <id> \t MapReduce job of in the application.");
    out.println("  --host <hostname> \t Hostname to push the application to.");
    out.println("  --apikey <key> \t Apikey of the account.");
  }

  public static void main(String[] args) throws TException, AppFabricServiceException {
    ReactorClient client;
    try {
      client = new ReactorClient();
      client.configure(CConfiguration.create(), args);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      return;
    }
    client.execute();
  }

  private void deploy(AppFabricService.Client client)
    throws IOException, TException, AppFabricServiceException, InterruptedException {
    File file = new File(this.resource);

    AuthToken dummyAuthToken = new AuthToken("ReactorClient");
    System.out.println(String.format("Deploying... :%s", this.resource));

    ResourceIdentifier identifier =
      client.init(dummyAuthToken, new ResourceInfo(DEVELOPER_ACCOUNT_ID,"", file.getName(), (int)file.getTotalSpace(),
                                                   file.lastModified()));

    Preconditions.checkNotNull(identifier, "Resource identifier is null");

    BufferFileInputStream is =
      new BufferFileInputStream(file.getAbsolutePath(), 100 * 1024);

    try {
      while (true) {
        byte[] toSubmit = is.read();
        if (toSubmit.length == 0) {
          break;
        }
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
