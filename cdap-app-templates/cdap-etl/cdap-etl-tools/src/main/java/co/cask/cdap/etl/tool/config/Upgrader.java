/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.tool.config;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.artifact.ArtifactVersionRange;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.etl.proto.UpgradeContext;
import co.cask.cdap.etl.proto.UpgradeableConfig;
import co.cask.cdap.etl.proto.v2.DataStreamsConfig;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.etl.tool.ETLVersion;
import co.cask.cdap.proto.artifact.AppRequest;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;

import java.util.Set;

/**
 * Upgrades an old hydrator config into a new one.
 */
public class Upgrader {
  public static final String BATCH_NAME = "cdap-etl-batch";
  public static final String DATA_PIPELINE_NAME = "cdap-data-pipeline";
  public static final String DATA_STREAMS_NAME = "cdap-data-streams";
  public static final Set<String> ARTIFACT_NAMES =
    ImmutableSet.of(BATCH_NAME, DATA_PIPELINE_NAME, DATA_STREAMS_NAME);
  private static final Gson GSON = new Gson();
  private static final ArtifactVersion LOWEST_VERSION = new ArtifactVersion("3.2.0");

  private final String newVersion;
  private final ArtifactVersionRange upgradeRange;
  private final UpgradeContext etlBatchContext;
  private final UpgradeContext dataPipelineContext;
  private final UpgradeContext dataStreamsContext;

  public Upgrader(NamespaceClient namespaceClient, ArtifactClient artifactClient,
                  String newVersion, boolean includeCurrentVersion, boolean downgrade) {
    this.etlBatchContext = new ClientUpgradeContext(namespaceClient, artifactClient, BATCH_NAME, newVersion);
    this.dataPipelineContext =
      new ClientUpgradeContext(namespaceClient, artifactClient, DATA_PIPELINE_NAME, newVersion);
    this.dataStreamsContext = new ClientUpgradeContext(namespaceClient, artifactClient, DATA_STREAMS_NAME, newVersion);
    this.upgradeRange = downgrade ?
      new ArtifactVersionRange(new ArtifactVersion(newVersion), includeCurrentVersion,
                               new ArtifactVersion(ETLVersion.getVersion()), true) :
      new ArtifactVersionRange(LOWEST_VERSION, true, new ArtifactVersion(newVersion), includeCurrentVersion);
    this.newVersion = newVersion;
  }

  public boolean shouldUpgrade(ArtifactSummary artifactSummary) {
    // check its the system etl artifacts
    if (artifactSummary.getScope() != ArtifactScope.SYSTEM) {
      return false;
    }

    if (!Upgrader.ARTIFACT_NAMES.contains(artifactSummary.getName())) {
      return false;
    }

    ArtifactVersion artifactVersion = new ArtifactVersion(artifactSummary.getVersion());
    return upgradeRange.versionIsInRange(artifactVersion);
  }

  public boolean upgrade(ArtifactSummary oldArtifact, String oldConfigStr,
                         UpgradeAction upgradeAction) throws Exception {

    String artifactName = oldArtifact.getName();
    if (!ARTIFACT_NAMES.contains(artifactName)) {
      return false;
    }

    ArtifactVersion artifactVersion = new ArtifactVersion(oldArtifact.getVersion());
    Integer majorVersion = artifactVersion.getMajor();
    Integer minorVersion = artifactVersion.getMinor();

    if (majorVersion == null || minorVersion == null || !shouldUpgrade(oldArtifact)) {
      return false;
    }

    ArtifactSummary newArtifact = new ArtifactSummary(artifactName, newVersion, ArtifactScope.SYSTEM);
    AppRequest<? extends ETLConfig> appRequest;
    switch (artifactName) {
      case BATCH_NAME:
        appRequest = new AppRequest<>(newArtifact, convertBatchConfig(majorVersion, minorVersion,
                                                                      oldConfigStr, etlBatchContext));
        break;
      case DATA_PIPELINE_NAME:
        appRequest = new AppRequest<>(newArtifact, convertBatchConfig(majorVersion, minorVersion,
                                                                      oldConfigStr, dataPipelineContext));
        break;
      case DATA_STREAMS_NAME:
        appRequest = new AppRequest<>(newArtifact, convertStreamsConfig(oldConfigStr));
        break;
      default:
        // can never happen
        throw new IllegalStateException("Unknown artifact " + artifactName);
    }

    return upgradeAction.upgrade(appRequest);
  }

  /**
   * Performs an action on the upgraded hydrator config.
   */
  public interface UpgradeAction {
    boolean upgrade(AppRequest<? extends ETLConfig> appRequest) throws Exception;
  }

  private DataStreamsConfig convertStreamsConfig(String configStr) {
    DataStreamsConfig config = GSON.fromJson(configStr, DataStreamsConfig.class);

    DataStreamsConfig.Builder builder = DataStreamsConfig.builder()
      .addConnections(config.getConnections())
      .setResources(config.getResources())
      .setDriverResources(config.getDriverResources())
      .setClientResources(config.getClientResources())
      .setBatchInterval(config.getBatchInterval())
      .setCheckpointDir(config.getCheckpointDir())
      .setNumOfRecordsPreview(config.getNumOfRecordsPreview());

    for (ETLStage stage : config.getStages()) {
      builder.addStage(stage.upgradeStage(dataStreamsContext));
    }
    return builder.build();
  }

  private ETLBatchConfig convertBatchConfig(int majorVersion, int minorVersion, String configStr,
                                            UpgradeContext upgradeContext) {
    UpgradeableConfig config;

    if (majorVersion == 3 && minorVersion == 2) {
      config = GSON.fromJson(configStr, co.cask.cdap.etl.proto.v0.ETLBatchConfig.class);
    } else if (majorVersion == 3 && minorVersion == 3) {
      config = GSON.fromJson(configStr, co.cask.cdap.etl.proto.v1.ETLBatchConfig.class);
    } else {
      // 3.4.x and up all have the same config format, but the plugin artifacts may need to be upgraded
      ETLBatchConfig batchConfig = GSON.fromJson(configStr, ETLBatchConfig.class);
      ETLBatchConfig.Builder builder = ETLBatchConfig.builder(batchConfig.getSchedule())
        .addConnections(batchConfig.getConnections())
        .setResources(batchConfig.getResources())
        .setDriverResources(batchConfig.getDriverResources())
        .setEngine(batchConfig.getEngine());
      // upgrade any of the plugin artifact versions if needed
      for (ETLStage postAction : batchConfig.getPostActions()) {
        builder.addPostAction(postAction.upgradeStage(upgradeContext));
      }
      for (ETLStage stage : batchConfig.getStages()) {
        builder.addStage(stage.upgradeStage(upgradeContext));
      }
      return builder.build();
    }

    while (config.canUpgrade()) {
      config = config.upgrade(upgradeContext);
    }
    return (ETLBatchConfig) config;
  }
}
