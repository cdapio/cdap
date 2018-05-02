/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.runtime.spi.provisioner.dataproc;

import co.cask.cdap.runtime.spi.provisioner.Cluster;
import com.google.gson.Gson;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Manual tool for testing out dataproc provisioning and deprovisioning.
 */
public class DataProcTool {
  private static final Gson GSON = new Gson();

  public static void main(String[] args) throws Exception {

    Options options = new Options()
      .addOption(new Option("h", "help", false, "Print this usage message."))
      .addOption(new Option("k", "serviceAccountKey", true, "Google cloud service account key (json format)."))
      .addOption(new Option("p", "project", true, "Google cloud project id."))
      .addOption(new Option("c", "configFile", true, "File all provisioner settings as a json object."))
      .addOption(new Option("i", "imageVersion", true, "The image version for the cluster. Defaults to 1.2."))
      .addOption(new Option("n", "name", true, "Name of the cluster."));

    CommandLineParser parser = new BasicParser();
    CommandLine commandLine = parser.parse(options, args);
    String[] commandArgs = commandLine.getArgs();
    String command = commandArgs.length > 0 ? commandArgs[0] : null;

    // if help is an option, or if there isn't a single 'upgrade' command, print usage and exit.
    if (commandLine.hasOption("h") || commandArgs.length != 1 ||
      (!"provision".equalsIgnoreCase(command) && !"details".equalsIgnoreCase(command) &&
        !"deprovision".equalsIgnoreCase(command))) {
      printUsage(options);
      System.exit(0);
    }

    if (!commandLine.hasOption('n')) {
      System.err.println("Cluster name must be specified.");
      printUsage(options);
      System.exit(-1);
    }

    DataProcConf conf;
    if (commandLine.hasOption('c')) {
      String configFilePath = commandLine.getOptionValue('c');
      File configFile = new File(configFilePath);
      if (!configFile.isFile()) {
        System.err.println(configFilePath + " is not a file.");
        System.exit(-1);
      }
      try (Reader reader = new FileReader(configFile)) {
        conf = GSON.fromJson(reader, DataProcConf.class);
      }
    } else {
      if (!commandLine.hasOption('k')) {
        System.err.println("Must specify a service account key or a config file.");
        printUsage(options);
        System.exit(-1);
      }
      if (!commandLine.hasOption('p')) {
        System.err.println("Must specify a project id or a config file.");
        printUsage(options);
        System.exit(-1);
      }
      Map<String, String> properties = new HashMap<>();
      properties.put("accountKey", commandLine.getOptionValue('k'));
      properties.put("projectId", commandLine.getOptionValue('p'));
      conf = DataProcConf.fromProperties(properties);
    }

    String imageVersion = commandLine.hasOption('i') ? commandLine.getOptionValue('i') : "1.2";

    String name = commandLine.getOptionValue('n');
    try (DataProcClient client = DataProcClient.fromConf(conf)) {
      if ("provision".equals(command)) {
        client.createCluster(name, imageVersion);
      } else if ("details".equals(command)) {
        Optional<Cluster> cluster = client.getCluster(name);
        if (cluster.isPresent()) {
          System.out.println(GSON.toJson(cluster));
        }
      } else if ("deprovision".equals(command)) {
        client.deleteCluster(name);
      }
    }
  }

  private static void printUsage(Options options) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp(
      DataProcTool.class.getSimpleName() + " provision|details|deprovision",
      "Provisions, deprovisions, or gets the status of a cluster. Basic provisioner settings can be passed in as " +
        "options. Advanced provisioner settings can be specified in a file, in which case every setting must be " +
        "given.",
      options, "");
  }
}
