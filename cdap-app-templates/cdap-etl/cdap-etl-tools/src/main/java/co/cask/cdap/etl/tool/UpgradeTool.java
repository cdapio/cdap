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

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.etl.proto.UpgradeContext;
import co.cask.cdap.etl.proto.UpgradeableConfig;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLRealtimeConfig;
import co.cask.cdap.etl.tool.config.BatchClientBasedUpgradeContext;
import co.cask.cdap.etl.tool.config.RealtimeClientBasedUpgradeContext;
import co.cask.cdap.proto.ApplicationDetail;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.security.authentication.client.AccessToken;
import com.google.common.collect.ImmutableSet;
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
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

/**
 * Used to upgrade pipelines from older pipelines to current pipelines.
 */
public class UpgradeTool {
  private static final Logger LOG = LoggerFactory.getLogger(UpgradeTool.class);
  private static final String BATCH_NAME = "cdap-etl-batch";
  private static final String REALTIME_NAME = "cdap-etl-realtime";
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final int DEFAULT_READ_TIMEOUT_MILLIS = 90 * 1000;
  private final NamespaceClient namespaceClient;
  private final ApplicationClient appClient;
  private final ArtifactSummary batchArtifact;
  private final ArtifactSummary realtimeArtifact;
  private final UpgradeContext batchUpgradeContext;
  private final UpgradeContext realtimeUpgradeContext;

  private UpgradeTool(ClientConfig clientConfig) {
    String version = ETLVersion.getVersion();
    this.batchArtifact = new ArtifactSummary(BATCH_NAME, version, ArtifactScope.SYSTEM);
    this.realtimeArtifact = new ArtifactSummary(REALTIME_NAME, version, ArtifactScope.SYSTEM);
    this.appClient = new ApplicationClient(clientConfig);
    this.namespaceClient = new NamespaceClient(clientConfig);
    ArtifactClient artifactClient = new ArtifactClient(clientConfig);
    this.batchUpgradeContext = new BatchClientBasedUpgradeContext(artifactClient);
    this.realtimeUpgradeContext = new RealtimeClientBasedUpgradeContext(artifactClient);
  }

  private Set<Id.Application> upgrade() throws Exception {
    Set<Id.Application> upgraded = new HashSet<>();
    for (NamespaceMeta namespaceMeta : namespaceClient.list()) {
      Id.Namespace namespace = Id.Namespace.from(namespaceMeta.getName());
      upgraded.addAll(upgrade(namespace));
    }
    return upgraded;
  }

  private Set<Id.Application> upgrade(Id.Namespace namespace) throws Exception {
    Set<Id.Application> upgraded = new HashSet<>();
    Set<String> artifactNames = ImmutableSet.of(BATCH_NAME, REALTIME_NAME);
    for (ApplicationRecord appRecord : appClient.list(namespace, artifactNames, null)) {
      Id.Application appId = Id.Application.from(namespace, appRecord.getName());
      if (upgrade(appId)) {
        upgraded.add(appId);
      }
    }
    return upgraded;
  }

  private boolean upgrade(Id.Application appId) throws Exception {
    ApplicationDetail appDetail = appClient.get(appId);

    if (!shouldUpgrade(appDetail.getArtifact())) {
      return false;
    }

    LOG.info("Upgrading pipeline: {}", appId);
    String artifactName = appDetail.getArtifact().getName();
    String artifactVersion = appDetail.getVersion();
    if (BATCH_NAME.equals(artifactName)) {
      ETLBatchConfig upgradedConfig =
        convertBatchConfig(artifactVersion, appDetail.getConfiguration(), batchUpgradeContext);
      AppRequest<ETLBatchConfig> updateRequest = new AppRequest<>(batchArtifact, upgradedConfig);
      appClient.update(appId, updateRequest);
    } else if (REALTIME_NAME.equals(artifactName)) {
      ETLRealtimeConfig upgradedConfig =
        convertRealtimeConfig(artifactVersion, appDetail.getConfiguration(), realtimeUpgradeContext);
      AppRequest<ETLRealtimeConfig> updateRequest = new AppRequest<>(realtimeArtifact, upgradedConfig);
      appClient.update(appId, updateRequest);
    } else {
      // should never happen
      LOG.warn("Unknown app artifact {}. Skipping pipeline.", artifactName);
      return false;
    }
    return true;
  }

  public static void main(String[] args) throws Exception {

    Options options = new Options()
      .addOption(new Option("h", "help", false, "Print this usage message."))
      .addOption(new Option("u", "uri", true, "CDAP instance URI to interact with in the format " +
        "[http[s]://]<hostname>:<port>. Defaults to localhost:10000."))
      .addOption(new Option("a", "accesstoken", true, "File containing the access token to use when interacting " +
        "with a secure CDAP instance."))
      .addOption(new Option("t", "timeout", true, "Timeout in milliseconds to use when interacting with the " +
        "CDAP RESTful APIs. Defaults to " + DEFAULT_READ_TIMEOUT_MILLIS + "."))
      .addOption(new Option("n", "namespace", true, "Namespace to perform the upgrade in. If none is given, " +
        "pipelines in all namespaces will be upgraded."))
      .addOption(new Option("p", "pipeline", true, "Name of the pipeline to upgrade. If specified, a namespace " +
        "must also be given."))
      .addOption(new Option("f", "configfile", true, "File containing old application details to update. " +
        "The file contents are expected to be in the same format as the request body for creating an " +
        "ETL application from one of the etl artifacts. " +
        "It is expected to be a JSON Object containing 'artifact' and 'config' fields." +
        "The value for 'artifact' must be a JSON Object that specifies the artifact scope, name, and version. " +
        "The value for 'config' must be a JSON Object specifies the source, transforms, and sinks of the pipeline, " +
        "as expected by older versions of the etl artifacts."))
      .addOption(new Option("o", "outputfile", true, "File to write the converted application details provided in " +
        "the configfile option. If none is given, results will be written to the input file + '.converted'. " +
        "The contents of this file can be sent directly to CDAP to update or create an application."));

    CommandLineParser parser = new BasicParser();
    CommandLine commandLine = parser.parse(options, args);
    String[] commandArgs = commandLine.getArgs();

    // if help is an option, or if there isn't a single 'upgrade' command, print usage and exit.
    if (commandLine.hasOption("h") || commandArgs.length != 1 || !"upgrade".equalsIgnoreCase(commandArgs[0])) {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp(
        UpgradeTool.class.getName() + " upgrade",
        "Upgrades Hydrator pipelines created for 3.2.x versions" +
        "of the cdap-etl-batch and cdap-etl-realtime artifacts into pipelines compatible with 3.3.x versions of " +
        "cdap-etl-batch and cdap-etl-realtime. Connects to an instance of CDAP to find any 3.2.x pipelines, then " +
        "upgrades those pipelines.", options, "");
      System.exit(0);
    }

    ClientConfig clientConfig = getClientConfig(commandLine);

    if (commandLine.hasOption("f")) {
      String inputFilePath = commandLine.getOptionValue("f");
      String outputFilePath = commandLine.hasOption("o") ? commandLine.getOptionValue("o") : inputFilePath + ".new";
      convertFile(inputFilePath, outputFilePath, new ArtifactClient(clientConfig));
      System.exit(0);
    }

    UpgradeTool upgradeTool = new UpgradeTool(clientConfig);

    String namespace = commandLine.getOptionValue("n");
    String pipelineName = commandLine.getOptionValue("p");

    if (pipelineName != null) {
      if (namespace == null) {
        throw new IllegalArgumentException("Must specify a namespace when specifying a pipeline.");
      }
      Id.Application appId = Id.Application.from(namespace, pipelineName);
      if (upgradeTool.upgrade(appId)) {
        LOG.info("Successfully upgraded {}.", appId);
      } else {
        LOG.info("{} did not need to be upgraded.", appId);
      }
      System.exit(0);
    }

    if (namespace != null) {
      printUpgraded(upgradeTool.upgrade(Id.Namespace.from(namespace)));
      System.exit(0);
    }

    printUpgraded(upgradeTool.upgrade());
  }

  private static void printUpgraded(Set<Id.Application> pipelines) {
    if (pipelines.size() == 0) {
      LOG.info("Did not find any pipelines that needed upgrading.");
      return;
    }
    LOG.info("Successfully upgraded {} pipelines:", pipelines.size());
    for (Id.Application pipeline : pipelines) {
      LOG.info("-- {}", pipeline);
    }
  }

  private static ClientConfig getClientConfig(CommandLine commandLine) throws IOException {
    String uriStr = commandLine.hasOption("u") ? commandLine.getOptionValue("u") : "localhost:10000";
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

  private static void convertFile(String configFilePath, String outputFilePath,
                                  ArtifactClient artifactClient) throws Exception {
    File configFile = new File(configFilePath);
    if (!configFile.exists()) {
      throw new IllegalArgumentException(configFilePath + " does not exist.");
    }
    if (!configFile.isFile()) {
      throw new IllegalArgumentException(configFilePath + " is not a file.");
    }
    String fileContents = new String(Files.readAllBytes(configFile.toPath()), StandardCharsets.UTF_8);

    ETLAppRequest artifactFile = GSON.fromJson(fileContents, ETLAppRequest.class);
    if (!shouldUpgrade(artifactFile.artifact)) {
      throw new IllegalArgumentException(
        "Cannot update for artifact " + artifactFile.artifact + ". " +
          "Please check the artifact is cdap-etl-batch or cdap-etl-realtime in the system scope of version 3.2.x.");
    }

    String version = ETLVersion.getVersion();

    File outputFile = new File(outputFilePath);
    String oldArtifactVersion = artifactFile.artifact.getVersion();
    if (BATCH_NAME.equals(artifactFile.artifact.getName())) {
      ArtifactSummary artifact = new ArtifactSummary(BATCH_NAME, version, ArtifactScope.SYSTEM);
      UpgradeContext upgradeContext = new BatchClientBasedUpgradeContext(artifactClient);
      ETLBatchConfig config = convertBatchConfig(oldArtifactVersion, artifactFile.config.toString(), upgradeContext);
      AppRequest<ETLBatchConfig> updated = new AppRequest<>(artifact, config);
      try (BufferedWriter writer = Files.newBufferedWriter(outputFile.toPath(), StandardCharsets.UTF_8)) {
        writer.write(GSON.toJson(updated));
      }
    } else {
      ArtifactSummary artifact = new ArtifactSummary(REALTIME_NAME, version, ArtifactScope.SYSTEM);
      UpgradeContext upgradeContext = new RealtimeClientBasedUpgradeContext(artifactClient);
      ETLRealtimeConfig config =
        convertRealtimeConfig(oldArtifactVersion, artifactFile.config.toString(), upgradeContext);
      AppRequest<ETLRealtimeConfig> updated = new AppRequest<>(artifact, config);
      try (BufferedWriter writer = Files.newBufferedWriter(outputFile.toPath(), StandardCharsets.UTF_8)) {
        writer.write(GSON.toJson(updated));
      }
    }

    LOG.info("Successfully converted application details from file " + configFilePath + ". " +
               "Results have been written to " + outputFilePath);
  }

  // class used to deserialize the old pipeline requests
  @SuppressWarnings("unused")
  private static class ETLAppRequest {
    private ArtifactSummary artifact;
    private JsonObject config;
  }

  private static boolean shouldUpgrade(ArtifactSummary artifactSummary) {
    // check its the system etl artifacts
    if (artifactSummary.getScope() != ArtifactScope.SYSTEM) {
      return false;
    }

    if (!BATCH_NAME.equals(artifactSummary.getName()) && !REALTIME_NAME.equals(artifactSummary.getName())) {
      return false;
    }

    // check its 3.x.x versions of etl
    ArtifactVersion artifactVersion = new ArtifactVersion(artifactSummary.getVersion());
    Integer majorVersion = artifactVersion.getMajor();
    Integer minorVersion = artifactVersion.getMinor();
    return majorVersion != null && majorVersion == 3 && minorVersion != null && minorVersion >= 2;
  }

  private static ETLBatchConfig convertBatchConfig(String artifactVersion, String configStr,
                                                   UpgradeContext upgradeContext) {
    UpgradeableConfig config;
    if (artifactVersion.startsWith("3.2.")) {
      config = GSON.fromJson(configStr, co.cask.cdap.etl.proto.v0.ETLBatchConfig.class);
    } else if (artifactVersion.startsWith("3.3.")) {
      config = GSON.fromJson(configStr, co.cask.cdap.etl.proto.v1.ETLBatchConfig.class);
    } else {
      LOG.warn("Unknown artifact version {}. Skipping pipeline.", artifactVersion);
      // should never happen
      return null;
    }

    while (config.canUpgrade()) {
      config = config.upgrade(upgradeContext);
    }
    return (ETLBatchConfig) config;
  }

  private static ETLRealtimeConfig convertRealtimeConfig(String artifactVersion, String configStr,
                                                         UpgradeContext upgradeContext) {
    UpgradeableConfig config;
    if (artifactVersion.startsWith("3.2.")) {
      config = GSON.fromJson(configStr, co.cask.cdap.etl.proto.v0.ETLRealtimeConfig.class);
    } else if (artifactVersion.startsWith("3.3.")) {
      config = GSON.fromJson(configStr, co.cask.cdap.etl.proto.v1.ETLRealtimeConfig.class);
    } else {
      LOG.warn("Unknown artifact version {}. Skipping pipeline.", artifactVersion);
      // should never happen
      return null;
    }

    while (config.canUpgrade()) {
      config = config.upgrade(upgradeContext);
    }
    return (ETLRealtimeConfig) config;
  }
}
