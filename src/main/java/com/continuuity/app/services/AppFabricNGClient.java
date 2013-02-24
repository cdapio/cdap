/*
* Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
*/
package com.continuuity.app.services;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.Copyright;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.commons.cli.*;

import java.io.PrintStream;
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
 * client.run();
 */
public class AppFabricNGClient {

  private static Set<String> availableCommands = Sets.newHashSet("deploy", "stop", "start", "promote", "verify", "status");
  private final String RESOURCE_ARG = "resource";
  private final String APPLICATION_ARG = "application";
  private final String PROCESSOR_ARG = "processor";
  private final String VPC_ARG = "vpc";
  private final String AUTH_TOKEN_ARG = "authtoken";

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

  public void run() {
    Preconditions.checkNotNull(command, "App client is not configured to run");
  }

  public AppFabricNGClient() {

  }

  public String configure(CConfiguration configuration, String args[]) throws ParseException {

    this.configuration = configuration;

    Preconditions.checkArgument(args.length > 1, "Not enough arguments");

    boolean knownCommand = availableCommands.contains(args[0]);
    Preconditions.checkArgument(knownCommand, "Unknown Command specified");
    command = args[0];

    CommandLineParser commandLineParser = new GnuParser();
    Options options = new Options();
    options.addOption(RESOURCE_ARG, true, "Jar that contains the application");
    options.addOption(APPLICATION_ARG, true, "TOADD APPROPRIATE DESCRIPTION"); //TODO:
    options.addOption(PROCESSOR_ARG, true, "TOADD APPROPRIATE DESCRIPTION"); //TODO:
    options.addOption(VPC_ARG, true, "VPC to push the application");
    options.addOption(AUTH_TOKEN_ARG, true, "Auth token of the account");
    CommandLine commandLine = commandLineParser.parse(options, Arrays.copyOfRange(args, 1, args.length));

    //Check if the appropriate args are passed in for each of the commands
    if ("deploy".equals(command)) {
      Preconditions.checkArgument(commandLine.hasOption(RESOURCE_ARG), "deploy command should have resource argument");
      this.resource = commandLine.getOptionValue(RESOURCE_ARG);
    }
    if ("start".equals(command)) {
      Preconditions.checkArgument(commandLine.hasOption(APPLICATION_ARG), "start command should have application argument");
      Preconditions.checkArgument(commandLine.hasOption(PROCESSOR_ARG), "start command should have processor argument");
      this.application = commandLine.getOptionValue(APPLICATION_ARG);
      this.processor = commandLine.getOptionValue(PROCESSOR_ARG);
    }
    if ("stop".equals(command)) {
      Preconditions.checkArgument(commandLine.hasOption(APPLICATION_ARG), "stop command should have application argument");
      Preconditions.checkArgument(commandLine.hasOption(PROCESSOR_ARG), "stop command should have processor argument");
      this.application = commandLine.getOptionValue(APPLICATION_ARG);
      this.processor = commandLine.getOptionValue(PROCESSOR_ARG);

    }
    if ("status".equals(command)) {
      Preconditions.checkArgument(commandLine.hasOption(APPLICATION_ARG), "status command should have " +
        "application argument");
      Preconditions.checkArgument(commandLine.hasOption(PROCESSOR_ARG), "status command should have processor argument");
      this.application = commandLine.getOptionValue(APPLICATION_ARG);
      this.processor = commandLine.getOptionValue(PROCESSOR_ARG);

    }
    if ("verify".equals(command)) {
      Preconditions.checkArgument(commandLine.hasOption(RESOURCE_ARG), "verify command should have resource argument");
      this.resource = commandLine.getOptionValue(RESOURCE_ARG);

    }
    if ("promote".equals(command)) {
      Preconditions.checkArgument(commandLine.hasOption(VPC_ARG), "promote command should have vpc argument");
      Preconditions.checkArgument(commandLine.hasOption(AUTH_TOKEN_ARG), "promote command should have auth token argument");
      Preconditions.checkArgument(commandLine.hasOption(APPLICATION_ARG), "promote command should have" +
        " application argument");
      this.vpc = commandLine.getOptionValue(VPC_ARG);
      this.authToken = commandLine.getOptionValue(AUTH_TOKEN_ARG);
      this.application = commandLine.getOptionValue(APPLICATION_ARG);


    }
    return command;
  }

  public static void main(String[] args) {
    String command = null;
    AppFabricNGClient client = null;
    try {
      client = new AppFabricNGClient();
      client.configure(CConfiguration.create(), args);
    } catch (Exception e) {
      usage();
      return;
    }

    client.run();
  }

  private static void usage() {
    PrintStream out = System.out;
    String name = "app-fabric";
    Copyright.print(out);
    out.println("Usage: ");
    out.println("  " + name + " deploy     --resource <jar>");
    out.println("  " + name + " verify     --resource <jar>");
    out.println("  " + name + " start      --application <app_id> --processor <?>"); //TODO:
    out.println("  " + name + " stop       --application <app_id> --processor <?>"); //TODO:
    out.println("  " + name + " status     --application <app_id> --processor <?>"); //TODO
    out.println("  " + name + " promote    --vpc <vpc_name> --authtoken <auth_token> --application <app_id>");
  }
}
