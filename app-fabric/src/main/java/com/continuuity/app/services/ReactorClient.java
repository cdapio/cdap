/*
* Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
*/
package com.continuuity.app.services;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.Copyright;
import com.continuuity.common.utils.UsageException;
import com.continuuity.internal.app.BufferFileInputStream;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Client for interacting with Local Reactor's app-fabric service to perform the following operations:
 * a) Deploy app
 * b) Start/Stop/Status of flow, procedure or map reduce job
 * c) Promote app to cloud
 * d) Scale number of flowlet instances
 * <p/>
 * Usage:
 * ReactorClient client = new ReactorClient();
 * client.execute(args, CConfiguration.create());
 */

public final class ReactorClient {

  /**
   * For debugging purposes, should only be set to true in unit tests.
   * When true, program will print the stack trace after the usage.
   */
  public static boolean debug = false;

  private static final String DEVELOPER_ACCOUNT_ID = Constants.DEVELOPER_ACCOUNT_ID;
  private static final Set<String> AVAILABLE_COMMANDS = Sets.newHashSet("deploy", "stop", "start", "help", "promote",
                                                                        "status", "scale", "delete");
  private static final String ARCHIVE_LONG_OPT_ARG = "archive";
  private static final String APPLICATION_LONG_OPT_ARG = "application";
  private static final String PROCEDURE_LONG_OPT_ARG = "procedure";
  private static final String FLOW_LONG_OPT_ARG = "flow";
  private static final String FLOWLET_LONG_OPT_ARG = "flowlet";
  private static final String FLOWLET_INSTANCES_LONG_OPT_ARG = "instances";
  private static final String MAPREDUCE_LONG_OPT_ARG = "mapreduce";
  private static final String HOSTNAME_LONG_OPT_ARG = "host";
  private static final String REMOTE_LONG_OPT_ARG = "remote";
  private static final String APIKEY_LONG_OPT_ARG = "apikey";
  private static final String DEBUG_LONG_OPT_ARG = "debug";

  private String resource;
  private String application;
  private String procedure;
  private String flow;
  private String flowlet;
  private short flowletInstances;
  private String mapReduce;
  private String hostname;
  private String remoteHostname;
  private String authToken;
  private Properties runtimeArguments = new Properties();

  private String command;

  private CConfiguration configuration;

  String getCommand() {
    return command;
  }

  /**
   * Prints the usage information and throws a UsageException if error is true.
   */
  private void usage(boolean error) {
    PrintStream out;
    if (error) {
      out = System.err;
    } else {
      out = System.out;
    }
    Copyright.print(out);
    out.println("Usage:");
    out.println("  reactor-client deploy    --archive <filename> [--host <hostname>]");
    out.println("  reactor-client start     --application <id> ( --flow <id> | --procedure <id> | --mapreduce <id>)  " +
                  "[--host <hostname>] [-R<property>=<value>]");
    out.println("  reactor-client stop      --application <id> ( --flow <id> | --procedure <id> | --mapreduce <id>) " +
                  "[--host <hostname>]");
    out.println("  reactor-client status    --application <id> ( --flow <id> | --procedure <id> | --mapreduce <id>) " +
                  "[--host <hostname>]");
    out.println("  reactor-client scale     --application <id> --flow <id> --flowlet <id> --instances <number> " +
                  "[--host <hostname>]");
    out.println("  reactor-client promote   --application <id> --remote <hostname> --apikey <key>");
    out.println("  reactor-client delete    --application <id> [--host <hostname>]");
    out.println("  reactor-client help");

    out.println("Options:");
    out.println("  --archive <filename> \t Archive containing the application.");
    out.println("  --application <id> \t Application Id.");
    out.println("  --flow <id> \t\t Flow id of the application.");
    out.println("  --procedure <id> \t Procedure of in the application.");
    out.println("  --mapreduce <id> \t MapReduce job of in the application.");
    out.println("  --host <hostname> \t Specifies reactor host [Default: localhost]");
    out.println("  --remote <hostname> \t Specifies remote reactor host");
    out.println("  --apikey <key> \t Apikey of the account.");
    out.flush();
    if (error) {
      throw new UsageException();
    }
  }

  /**
   * Prints an error message followed by the usage information.
   *
   * @param errorMessage the error message
   */
  private void usage(String errorMessage) {
    if (errorMessage != null) {
      System.err.println("Error: " + errorMessage);
    }
    usage(true);
  }

  /**
   * Executes the configured operation
   */
  private void executeInternal() throws TException, InterruptedException, AppFabricServiceException, IOException {
    Preconditions.checkNotNull(command, "App client is not configured to run");
    Preconditions.checkNotNull(configuration, "App client configuration is not set");

    int port = configuration.getInt(Constants.CFG_APP_FABRIC_SERVER_PORT, Constants.DEFAULT_APP_FABRIC_SERVER_PORT);
    TTransport transport = null;
    TProtocol protocol;

    try {
      transport = new TFramedTransport(new TSocket(hostname, port));
      protocol = new TBinaryProtocol(transport);
      transport.open();
      AppFabricService.Client client = new AppFabricService.Client(protocol);

      if ("deploy".equals(command)) {
        deploy(client);
      }

      if ("delete".equals(command)) {
        System.out.print("Deleting the app...");
        AuthToken dummyAuthToken = new AuthToken("ReactorClient");
        client.removeApplication(dummyAuthToken, new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, "", 1));
        System.out.println("Deleted");
      }

      if ("start".equals(command)) {
        AuthToken dummyAuthToken = new AuthToken("ReactorClient");
        FlowIdentifier identifier;
        if (flow != null) {
          identifier = new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, flow, 1);
          identifier.setType(EntityType.FLOW);
          System.out.print(String.format("Starting flow %s for application %s...", flow, application));
        } else if (mapReduce != null) {
          identifier = new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, mapReduce, 1);
          identifier.setType(EntityType.MAPREDUCE);
          System.out.print(String.format("Starting mapreduce job %s for application %s...", mapReduce, application));
        } else {
          identifier = new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, procedure, 1);
          identifier.setType(EntityType.QUERY);
          System.out.print(String.format("Starting procedure %s for application %s...", procedure, application));
        }

        Map<String, String> arguments = Maps.newHashMap();
        for (Map.Entry<Object, Object> entry : runtimeArguments.entrySet()) {
          arguments.put((String) entry.getKey(), (String) entry.getValue());
        }
        client.start(dummyAuthToken, new FlowDescriptor(identifier, arguments));
        System.out.println("Started");
        return;
      }

      if ("stop".equals(command)) {
        AuthToken dummyAuthToken = new AuthToken("ReactorClient");
        FlowIdentifier identifier;
        if (flow != null) {
          identifier = new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, flow, 1);
          identifier.setType(EntityType.FLOW);
          System.out.print(String.format("Stopping flow %s for application %s...", flow, application));
        } else if (mapReduce != null) {
          identifier = new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, mapReduce, 1);
          identifier.setType(EntityType.MAPREDUCE);
          System.out.print(String.format("Killing mapreduce job %s for application %s...", mapReduce, application));
        } else {
          identifier = new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, procedure, 1);
          identifier.setType(EntityType.QUERY);
          System.out.print(String.format("Stopping procedure %s for application %s...", procedure, application));
        }
        client.stop(dummyAuthToken, identifier);
        System.out.println("Stopped");
        return;
      }

      if ("scale".equals(command)) {
        AuthToken dummyAuthToken = new AuthToken("ReactorClient");
        FlowIdentifier identifier = new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, flow, 1);
        identifier.setType(EntityType.FLOW);
        System.out.println(String.format("Changing number of flowlet instances for flowlet %s "
                                           + "in flow %s of application %s ", flowlet, flow, application));
        client.setInstances(dummyAuthToken, identifier, flowlet, flowletInstances);
        System.out.println("The number of flowlet instances has been changed.");
        return;
      }

      if ("promote".equals(command)) {
        System.out.print("Promoting to the reactor '" + remoteHostname + "'...");
        ResourceIdentifier identifier = new ResourceIdentifier(DEVELOPER_ACCOUNT_ID, application, "noresource", 1);
        boolean status = client.promote(new AuthToken(authToken), identifier, remoteHostname);
        if (status) {
          System.out.println("OK");
        } else {
          System.err.println("Failed");
        }
      }
      if ("status".equals(command)) {
        AuthToken dummyAuthToken = new AuthToken("ReactorClient");
        FlowIdentifier identifier;
        if (flow != null) {
          identifier = new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, flow, 1);
          identifier.setType(EntityType.FLOW);
          System.out.print(String.format("Getting status for flow %s in application %s...", flow, application));
        } else {
          identifier = new FlowIdentifier(DEVELOPER_ACCOUNT_ID, application, procedure, 1);
          identifier.setType(EntityType.QUERY);
          System.out.print(String.format("Getting status for procedure %s in application %s...", flow, application));
        }
        FlowStatus flowStatus = client.status(dummyAuthToken, identifier);
        Preconditions.checkNotNull(flowStatus, "Problem getting the status the application");
        System.out.println(String.format("%s", flowStatus.getStatus()));
      }
    } catch (TTransportException e) {
      System.err.println("Unable to connect to host '" + hostname + "'");
    } finally {
      transport.close();
    }
  }

  /**
   * Parses the provided command.
   */
  String parseArguments(String[] args, CConfiguration config) {
    configuration = config;

    if (args.length == 0) { // command line arguments are missing
      usage(true); // usage method throws new UsageException() when called with true!
    }

    command = args[0];

    if ("help".equals(command)) {
      usage(false);
      return "help";
    }

    if (!AVAILABLE_COMMANDS.contains(command)) {
      usage(false);
      System.err.println(String.format("Unsupported command: %s", command));
      return "help";
    }

    CommandLineParser commandLineParser = new GnuParser();

    Options options = new Options();

    options.addOption("a", APPLICATION_LONG_OPT_ARG, true, "Application Id.");
    options.addOption("r", ARCHIVE_LONG_OPT_ARG, true, "Archive that contains the application.");
    options.addOption("p", PROCEDURE_LONG_OPT_ARG, true, "Procedure Id.");
    options.addOption("h", HOSTNAME_LONG_OPT_ARG, true, "Specifies local reactor hostname [Default: localhost]");
    options.addOption("r", REMOTE_LONG_OPT_ARG, true, "Specifies remote reactor hostname");
    options.addOption("k", APIKEY_LONG_OPT_ARG, true, "Apikey of the account.");
    options.addOption("f", FLOW_LONG_OPT_ARG, true, "Flow Id.");
    options.addOption("l", FLOWLET_LONG_OPT_ARG, true, "Flowlet Id.");
    options.addOption("i", FLOWLET_INSTANCES_LONG_OPT_ARG, true, "Flowlet Instances.");
    options.addOption("m", MAPREDUCE_LONG_OPT_ARG, true, "MapReduce job Id.");
    options.addOption("d", DEBUG_LONG_OPT_ARG, false, "Debug");

    Option runtimeArgOptions = OptionBuilder.withArgName("property=value")
      .hasArgs(2)
      .withValueSeparator()
      .withDescription("use value for a given property")
      .create("R");
    options.addOption(runtimeArgOptions);

    try {
      CommandLine commandLine = commandLineParser.parse(options, Arrays.copyOfRange(args, 1, args.length));
      debug = commandLine.getOptionValue(DEBUG_LONG_OPT_ARG) == null ? false : true;

      hostname = "localhost";
      if (commandLine.hasOption(HOSTNAME_LONG_OPT_ARG)) {
        hostname = commandLine.getOptionValue(HOSTNAME_LONG_OPT_ARG);
      }

      System.out.println("Connecting to '" + hostname + "'");

      //Check if the appropriate args are passed in for each of the commands
      if ("deploy".equals(command)) {
        Preconditions.checkArgument(commandLine.hasOption(ARCHIVE_LONG_OPT_ARG),
                                    "deploy command should have archive argument");
        resource = commandLine.getOptionValue(ARCHIVE_LONG_OPT_ARG);

      } else if ("delete".equals(command)) {
        Preconditions.checkArgument(commandLine.hasOption(APPLICATION_LONG_OPT_ARG), "status command should have " +
          "application argument");
        application = commandLine.getOptionValue(APPLICATION_LONG_OPT_ARG);

      } else if ("start".equals(command)) {
        Preconditions.checkArgument(commandLine.hasOption(APPLICATION_LONG_OPT_ARG), "status command should have " +
          "application argument");
        Preconditions.checkArgument(commandLine.hasOption(PROCEDURE_LONG_OPT_ARG) ||
                                      commandLine.hasOption(FLOW_LONG_OPT_ARG) ||
                                      commandLine.hasOption(MAPREDUCE_LONG_OPT_ARG),
                                    "start command should have procedure or flow or mapreduce argument");

        application = commandLine.getOptionValue(APPLICATION_LONG_OPT_ARG);
        procedure = commandLine.getOptionValue(PROCEDURE_LONG_OPT_ARG);
        flow = commandLine.getOptionValue(FLOW_LONG_OPT_ARG);
        mapReduce = commandLine.getOptionValue(MAPREDUCE_LONG_OPT_ARG);
        runtimeArguments = commandLine.getOptionProperties("R");
      } else if ("stop".equals(command)) {
        Preconditions.checkArgument(commandLine.hasOption(APPLICATION_LONG_OPT_ARG), "status command should have " +
          "application argument");
        Preconditions.checkArgument(commandLine.hasOption(PROCEDURE_LONG_OPT_ARG) ||
                                      commandLine.hasOption(FLOW_LONG_OPT_ARG) ||
                                      commandLine.hasOption(MAPREDUCE_LONG_OPT_ARG),
                                    "stop command should have procedure or flow or mapreduce argument");

        application = commandLine.getOptionValue(APPLICATION_LONG_OPT_ARG);
        procedure = commandLine.getOptionValue(PROCEDURE_LONG_OPT_ARG);
        flow = commandLine.getOptionValue(FLOW_LONG_OPT_ARG);
        mapReduce = commandLine.getOptionValue(MAPREDUCE_LONG_OPT_ARG);

      } else if ("status".equals(command)) {
        Preconditions.checkArgument(commandLine.hasOption(APPLICATION_LONG_OPT_ARG), "status command should have " +
          "application argument");
        Preconditions.checkArgument(commandLine.hasOption(PROCEDURE_LONG_OPT_ARG) ||
                                      commandLine.hasOption(FLOW_LONG_OPT_ARG) ||
                                      commandLine.hasOption(MAPREDUCE_LONG_OPT_ARG),
                                    "status command should have procedure or flow or mapreduce argument");

        application = commandLine.getOptionValue(APPLICATION_LONG_OPT_ARG);
        procedure = commandLine.getOptionValue(PROCEDURE_LONG_OPT_ARG);
        flow = commandLine.getOptionValue(FLOW_LONG_OPT_ARG);
        mapReduce = commandLine.getOptionValue(MAPREDUCE_LONG_OPT_ARG);

      } else if ("scale".equals(command)) {
        Preconditions.checkArgument(commandLine.hasOption(APPLICATION_LONG_OPT_ARG), "status command should have " +
          "application argument");
        Preconditions.checkArgument(commandLine.hasOption(FLOW_LONG_OPT_ARG),
                                    "status command should have flow argument");
        Preconditions.checkArgument(commandLine.hasOption(FLOWLET_LONG_OPT_ARG),
                                    "status command should have flowlet argument");
        Preconditions.checkArgument(commandLine.hasOption(FLOWLET_INSTANCES_LONG_OPT_ARG),
                                    "status command should have number of flowlet instances argument");

        application = commandLine.getOptionValue(APPLICATION_LONG_OPT_ARG);
        flow = commandLine.getOptionValue(FLOW_LONG_OPT_ARG);
        flowlet = commandLine.getOptionValue(FLOWLET_LONG_OPT_ARG);

        flowletInstances = Short.parseShort(commandLine.getOptionValue(FLOWLET_INSTANCES_LONG_OPT_ARG));
        Preconditions.checkArgument(flowletInstances > 0, "number of flowlet instances needs to be greater than 0");

      } else if ("promote".equals(command)) {
        Preconditions.checkArgument(commandLine.hasOption(REMOTE_LONG_OPT_ARG), "promote command should have" +
          "remote reactor hostname");
        Preconditions.checkArgument(commandLine.hasOption(APIKEY_LONG_OPT_ARG), "promote command should " +
          "have auth token argument");
        Preconditions.checkArgument(commandLine.hasOption(APPLICATION_LONG_OPT_ARG), "promote command should have" +
          " application argument");

        remoteHostname = commandLine.getOptionValue(REMOTE_LONG_OPT_ARG);
        authToken = commandLine.getOptionValue(APIKEY_LONG_OPT_ARG);
        application = commandLine.getOptionValue(APPLICATION_LONG_OPT_ARG);
      }

    } catch (ParseException e) {
      usage(e.getMessage());
    } catch (IllegalArgumentException e) {
      usage(e.getMessage());
    } catch (Exception e) {
      usage(e.getMessage());
    }
    return command;
  }

  /**
   * This is actually the main method for this client, but in order to make it testable,
   * instead of exiting in case of error it returns null, whereas in case of success it
   * returns the executed command.
   *
   * @param args   the command line arguments of the main method
   * @param config The configuration of the gateway
   * @return null in case of error, a String representing the executed command
   *         in case of success
   */
  String execute(String[] args, CConfiguration config) {
    String command;
    try {
      command = parseArguments(args, config);
    } catch (UsageException e) {
      if (debug) { // this is mainly for debugging the unit test
        System.err.println("Exception for arguments: " + Arrays.toString(args) + ". Exception: " + e);
        e.printStackTrace(System.err);
      }
      return null;
    }
    try {
      executeInternal();
    } catch (Exception e) {
      System.err.println(String.format("\nError: %s", e.getMessage()));
      return null;
    }
    return command;
  }

  public static void main(String[] args) throws TException, AppFabricServiceException {
    // create a config and load the gateway properties
    CConfiguration config = CConfiguration.create();
    // create a data client and run it with the given arguments
    ReactorClient instance = new ReactorClient();
    String value = instance.execute(args, config);
    // exit with error in case fails
    if (value == null) {
      System.exit(1);
    }
  }

  private void deploy(AppFabricService.Client client)
    throws IOException, TException, AppFabricServiceException, InterruptedException {
    File file = new File(resource);

    AuthToken dummyAuthToken = new AuthToken("ReactorClient");
    System.out.print(String.format("Deploying application %s ...", resource));

    ResourceIdentifier identifier =
      client.init(dummyAuthToken, new ResourceInfo(DEVELOPER_ACCOUNT_ID, "", file.getName(), (int) file.getTotalSpace(),
                                                   file.lastModified()));

    Preconditions.checkNotNull(identifier, "Resource identifier is null");

    BufferFileInputStream is = new BufferFileInputStream(file.getAbsolutePath(), 100 * 1024);

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

    TimeUnit.SECONDS.sleep(5);

    DeploymentStatus status = client.dstatus(dummyAuthToken, identifier);

    if (DeployStatus.DEPLOYED.getCode() == status.getOverall()) {
      System.out.println("Deployed");
    } else if (DeployStatus.FAILED.getCode() == status.getOverall()) {
      System.err.println(String.format("Deployment failed: %s. Check Reactor log file for more details.",
                                       DeployStatus.FAILED.getMessage()));
    } else {
      System.err.println("Deployment taking more than 5 seconds. Please check the Reactor Dashboard for status");
    }
    return;
  }
}
