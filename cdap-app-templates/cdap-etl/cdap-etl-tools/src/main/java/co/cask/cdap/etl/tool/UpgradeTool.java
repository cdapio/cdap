/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.tool;

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.etl.proto.v2.ETLConfig;
import co.cask.cdap.etl.tool.config.Upgrader;
import co.cask.cdap.proto.ApplicationDetail;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.authentication.client.AccessToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Used to upgrade pipelines from older pipelines to current pipelines.
 */
public class UpgradeTool {
  private static final Logger LOG = LoggerFactory.getLogger(UpgradeTool.class);
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final int DEFAULT_READ_TIMEOUT_MILLIS = 90 * 1000;
  private final NamespaceClient namespaceClient;
  private final ApplicationClient appClient;
  private final Upgrader upgrader;
  @Nullable
  private final File errorDir;
  @Nullable
  private final File outputDir;
  private final boolean dryrun;

  private UpgradeTool(ClientConfig clientConfig, @Nullable File errorDir, @Nullable File outputDir,
                      String version, boolean includeCurrentVersion, boolean downgrade, boolean dryrun) {
    this.appClient = new ApplicationClient(clientConfig);
    this.namespaceClient = new NamespaceClient(clientConfig);
    this.errorDir = errorDir;
    this.outputDir = outputDir;
    this.upgrader = new Upgrader(namespaceClient, new ArtifactClient(clientConfig), version,
                                 includeCurrentVersion, downgrade);
    this.dryrun = dryrun;
  }

  private Set<ApplicationId> upgrade() throws Exception {
    Set<ApplicationId> upgraded = new HashSet<>();
    for (NamespaceMeta namespaceMeta : namespaceClient.list()) {
      NamespaceId namespace = new NamespaceId(namespaceMeta.getName());
      upgraded.addAll(upgrade(namespace));
    }
    return upgraded;
  }

  private Set<ApplicationId> upgrade(NamespaceId namespace) throws Exception {
    Set<ApplicationId> upgraded = new HashSet<>();
    for (ApplicationRecord appRecord : appClient.list(namespace, Upgrader.ARTIFACT_NAMES, null)) {
      ApplicationId appId = namespace.app(appRecord.getName());
      if (upgrade(appId)) {
        upgraded.add(appId);
      }
    }
    return upgraded;
  }

  private boolean upgrade(final ApplicationId appId) throws Exception {
    final ApplicationDetail appDetail = appClient.get(appId);

    if (!upgrader.shouldUpgrade(appDetail.getArtifact())) {
      LOG.debug("Skipping app {}.", appId);
      return false;
    }

    Upgrader.UpgradeAction action;
    if (dryrun) {
      action = new Upgrader.UpgradeAction() {
        @Override
        public boolean upgrade(AppRequest<? extends ETLConfig> appRequest) throws Exception {
          LOG.info("Writing dryrun app request for pipeline: {}", appId);
          String filename = String.format("%s-%s.json", appId.getNamespace(), appId.getApplication());
          File outputFile = new File(outputDir, filename);
          try (BufferedWriter writer = Files.newBufferedWriter(outputFile.toPath(), StandardCharsets.UTF_8)) {
            writer.write(GSON.toJson(appRequest));
          }
          return true;
        }
      };
    } else {
      action = new Upgrader.UpgradeAction() {
        @Override
        public boolean upgrade(AppRequest<? extends ETLConfig> appRequest) {
          LOG.info("Updating pipeline: {}", appId);
          try {
            appClient.update(appId, appRequest);
            return true;
          } catch (Exception e) {
            LOG.error("Error updating pipeline {}.", appId, e);
            if (errorDir != null) {
              File errorFile = new File(errorDir,
                                        String.format("%s-%s.json", appId.getParent(), appId.getEntityName()));
              LOG.error("Writing config for pipeline {} to {} for further manual investigation.",
                        appId, errorFile.getAbsolutePath());
              try (OutputStreamWriter outputStreamWriter = new OutputStreamWriter(new FileOutputStream(errorFile))) {
                outputStreamWriter.write(GSON.toJson(appRequest));
              } catch (IOException e1) {
                LOG.error("Error writing config out for manual investigation.", e1);
              }
            }
            return false;
          }
        }
      };
    }

    return upgrader.upgrade(appDetail.getArtifact(), appDetail.getConfiguration(), action);
  }

  public static void main(String[] args) throws Exception {

    Options options = new Options()
      .addOption(new Option("h", "help", false, "Print this usage message."))
      .addOption(new Option("u", "uri", true, "CDAP instance URI to interact with in the format " +
        "[http[s]://]<hostname>:<port>. Defaults to localhost:11015."))
      .addOption(new Option("a", "accesstoken", true, "File containing the access token to use when interacting " +
        "with a secure CDAP instance."))
      .addOption(new Option("t", "timeout", true, "Timeout in milliseconds to use when interacting with the " +
        "CDAP RESTful APIs. Defaults to " + DEFAULT_READ_TIMEOUT_MILLIS + "."))
      .addOption(new Option("n", "namespace", true, "Namespace to perform the upgrade in. If none is given, " +
        "pipelines in all namespaces will be upgraded."))
      .addOption(new Option("p", "pipeline", true, "Name of the pipeline to upgrade. If specified, a namespace " +
        "must also be given."))
      .addOption(new Option("v", "version", true, "Pipeline version to upgrade to. This should only be specified if " +
        "you want to upgrade to a version that is not the same as the version of this tool."))
      .addOption(new Option("r", "rerun", false, "Whether to re-run upgrade of pipelines. " +
        "This will re-run the upgrade for any pipelines that are using the current pipeline version in addition " +
        "to running upgrade for any old pipelines."))
      .addOption(new Option("f", "configfile", true, "File containing old application details to update. " +
        "The file contents are expected to be in the same format as the request body for creating an " +
        "ETL application from one of the etl artifacts. " +
        "It is expected to be a JSON Object containing 'artifact' and 'config' fields." +
        "The value for 'artifact' must be a JSON Object that specifies the artifact scope, name, and version. " +
        "The value for 'config' must be a JSON Object specifies the source, transforms, and sinks of the pipeline, " +
        "as expected by older versions of the etl artifacts."))
      .addOption(new Option("o", "outputfile", true, "File to write the converted application details provided in " +
        "the configfile option. If none is given, results will be written to the input file + '.converted'. " +
        "The contents of this file can be sent directly to CDAP to update or create an application."))
      .addOption(new Option("od", "outputdir", true, "Directory to write the application request that would be used " +
        "to upgrade the pipeline(s). This should only be used with the 'dryrun' command, not the 'upgrade' command. " +
        "The contents of the app request files can be sent directly to CDAP to update or create an application."))
      .addOption(new Option("e", "errorDir", true, "Optional directory to write any upgraded pipeline configs that " +
        "failed to upgrade. The problematic configs can then be manually edited and upgraded separately. " +
        "Upgrade errors may happen for pipelines that use plugins that are not backwards compatible. " +
        "This directory must be writable by the user that is running this tool."));

    CommandLineParser parser = new BasicParser();
    CommandLine commandLine = parser.parse(options, args);
    String[] commandArgs = commandLine.getArgs();
    String command = commandArgs.length > 0 ? commandArgs[0] : null;

    // if help is an option, or if there isn't a single 'upgrade' command, print usage and exit.
    if (commandLine.hasOption("h") || commandArgs.length != 1 ||
      (!"downgrade".equalsIgnoreCase(command) && !"upgrade".equalsIgnoreCase(command) &&
        !"dryrun".equalsIgnoreCase(command))) {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp(
        UpgradeTool.class.getName() + " upgrade|downgrade|dryrun",
        "Upgrades old pipelines to the current version. If the plugins used are not backward-compatible, " +
          "the attempted upgrade config will be written to the error directory for a manual upgrade. " +
          "If 'dryrun' is used as the command instead of 'upgrade', pipelines will not be upgraded, but the " +
          "application update requests will instead be written as files to the specified outputdir.",
        options, "");
      System.exit(0);
    }

    ClientConfig clientConfig = getClientConfig(commandLine);

    boolean downgrade = "downgrade".equalsIgnoreCase(command);
    String newVersion = commandLine.hasOption("v") ? commandLine.getOptionValue("v") : ETLVersion.getVersion();
    boolean includeCurrentVersion = commandLine.hasOption("r");
    if (commandLine.hasOption("f")) {
      String inputFilePath = commandLine.getOptionValue("f");
      String outputFilePath = commandLine.hasOption("o") ? commandLine.getOptionValue("o") : inputFilePath + ".new";
      convertFile(inputFilePath, outputFilePath, new Upgrader(new NamespaceClient(clientConfig),
                                                              new ArtifactClient(clientConfig),
                                                              newVersion, includeCurrentVersion, downgrade));
      System.exit(0);
    }

    File errorDir = commandLine.hasOption("e") ? new File(commandLine.getOptionValue("e")) : null;
    if (errorDir != null) {
      ensureDirExists(errorDir);
    }

    boolean dryrun = "dryrun".equalsIgnoreCase(command);
    File outputDir = null;
    if (dryrun) {
      if (!commandLine.hasOption("od")) {
        LOG.error("When performing a dryrun, an outputdir must be specified using the -od option.");
        System.exit(1);
      }
      outputDir = new File(commandLine.getOptionValue("od"));
      ensureDirExists(outputDir);
    }
    UpgradeTool upgradeTool = new UpgradeTool(clientConfig, errorDir, outputDir,
                                              newVersion, includeCurrentVersion, downgrade, dryrun);

    String namespace = commandLine.getOptionValue("n");
    String pipelineName = commandLine.getOptionValue("p");

    if (pipelineName != null) {
      if (namespace == null) {
        throw new IllegalArgumentException("Must specify a namespace when specifying a pipeline.");
      }
      ApplicationId appId = new ApplicationId(namespace, pipelineName);
      if (upgradeTool.upgrade(appId)) {
        LOG.info("Successfully {}d {}.", command, appId);
      } else {
        LOG.info("{} did not need to be {}d.", appId, command);
      }
      System.exit(0);
    }

    if (namespace != null) {
      printUpgraded(upgradeTool.upgrade(new NamespaceId(namespace)));
      System.exit(0);
    }

    printUpgraded(upgradeTool.upgrade());
  }

  private static void ensureDirExists(File dir) {
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        LOG.error("Unable to create directory {}.", dir.getAbsolutePath());
        System.exit(1);
      }
    } else if (!dir.isDirectory()) {
      LOG.error("{} is not a directory.", dir.getAbsolutePath());
    } else if (!dir.canWrite()) {
      LOG.error("Unable to write to directory {}.", dir.getAbsolutePath());
      System.exit(1);
    }
  }

  private static void printUpgraded(Set<ApplicationId> pipelines) {
    if (pipelines.size() == 0) {
      LOG.info("Did not find any pipelines that needed updating.");
      return;
    }
    LOG.info("Successfully updated {} pipelines:", pipelines.size());
    for (ApplicationId pipeline : pipelines) {
      LOG.info("  {}", pipeline);
    }
  }

  private static ClientConfig getClientConfig(CommandLine commandLine) throws IOException {
    String uriStr = commandLine.hasOption("u") ? commandLine.getOptionValue("u") : "localhost:11015";
    if (!uriStr.contains("://")) {
      uriStr = "http://" + uriStr;
    }
    URI uri = URI.create(uriStr);
    String hostname = uri.getHost();
    int port = uri.getPort();
    boolean sslEnabled = "https".equals(uri.getScheme());
    ConnectionConfig connectionConfig = ConnectionConfig.builder()
      .setHostname(hostname)
      .setPort(port)
      .setSSLEnabled(sslEnabled)
      .build();

    int readTimeout = commandLine.hasOption("t") ?
      Integer.parseInt(commandLine.getOptionValue("t")) : DEFAULT_READ_TIMEOUT_MILLIS;
    ClientConfig.Builder clientConfigBuilder = ClientConfig.builder()
      .setDefaultReadTimeout(readTimeout)
      .setConnectionConfig(connectionConfig);

    if (commandLine.hasOption("a")) {
      String tokenFilePath = commandLine.getOptionValue("a");
      File tokenFile = new File(tokenFilePath);
      if (!tokenFile.exists()) {
        throw new IllegalArgumentException("Access token file " + tokenFilePath + " does not exist.");
      }
      if (!tokenFile.isFile()) {
        throw new IllegalArgumentException("Access token file " + tokenFilePath + " is not a file.");
      }
      String tokenValue = new String(Files.readAllBytes(tokenFile.toPath()), StandardCharsets.UTF_8).trim();
      AccessToken accessToken = new AccessToken(tokenValue, 82000L, "Bearer");
      clientConfigBuilder.setAccessToken(accessToken);
    }

    return clientConfigBuilder.build();
  }

  private static void convertFile(String configFilePath, final String outputFilePath,
                                  Upgrader upgrader) throws Exception {
    File configFile = new File(configFilePath);
    if (!configFile.exists()) {
      throw new IllegalArgumentException(configFilePath + " does not exist.");
    }
    if (!configFile.isFile()) {
      throw new IllegalArgumentException(configFilePath + " is not a file.");
    }
    String fileContents = new String(Files.readAllBytes(configFile.toPath()), StandardCharsets.UTF_8);

    ETLAppRequest artifactFile = GSON.fromJson(fileContents, ETLAppRequest.class);

    if (!upgrader.shouldUpgrade(artifactFile.artifact)) {
      LOG.error("{} is not an artifact for an upgradeable Hydrator pipeline.", artifactFile.artifact);
      return;
    }

    upgrader.upgrade(artifactFile.artifact, artifactFile.config.toString(), new Upgrader.UpgradeAction() {
      @Override
      public boolean upgrade(AppRequest<? extends ETLConfig> appRequest) throws IOException {
        File outputFile = new File(outputFilePath);
        try (BufferedWriter writer = Files.newBufferedWriter(outputFile.toPath(), StandardCharsets.UTF_8)) {
          writer.write(GSON.toJson(appRequest));
        }
        return true;
      }
    });

    LOG.info("Successfully converted application details from file " + configFilePath + ". " +
               "Results have been written to " + outputFilePath);
  }

  // class used to deserialize the old pipeline requests
  @SuppressWarnings("unused")
  private static class ETLAppRequest {
    private ArtifactSummary artifact;
    private JsonObject config;
  }
}
