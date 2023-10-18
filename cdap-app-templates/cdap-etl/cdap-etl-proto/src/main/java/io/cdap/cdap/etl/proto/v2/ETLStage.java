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

package io.cdap.cdap.etl.proto.v2;

import io.cdap.cdap.api.app.ApplicationConfigUpdateAction;
import io.cdap.cdap.api.app.ApplicationUpdateContext;
import io.cdap.cdap.api.app.ApplicationValidationContext;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactVersionRange;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.UpgradeContext;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ETL Stage Configuration.
 */
public final class ETLStage {

  private final String name;
  private final ETLPlugin plugin;
  private final StageInformation information;

  // removed in 5.0.0, but keeping it here so that we can validate that nobody is trying to use it.
  private final String errorDatasetName;

  // Only for serialization/deserialization purpose for config upgrade to not lose data set by UI during update.
  private final Object inputSchema;
  private final Object outputSchema;
  private final String label;
  private final String id;

  private static final Logger LOG = LoggerFactory.getLogger(ETLStage.class);

  public ETLStage(String name, ETLPlugin plugin) {
    this.name = name;
    this.plugin = plugin;
    this.errorDatasetName = null;
    inputSchema = null;
    outputSchema = null;
    label = null;
    id = null;
    information = null;
  }

  // Used only for upgrade stage purpose.
  private ETLStage(String name, ETLPlugin plugin, String label, Object inputSchema,
      Object outputSchema, String id,
      StageInformation info) {
    this.name = name;
    this.plugin = plugin;
    this.errorDatasetName = null;
    this.inputSchema = inputSchema;
    this.outputSchema = outputSchema;
    this.label = label;
    this.id = id;
    this.information = info;
  }

  public String getName() {
    return name;
  }

  public ETLPlugin getPlugin() {
    return plugin;
  }

  /**
   * Validate correctness. Since this object is created through deserialization, some fields that
   * should not be null may be null.
   *
   * @throws IllegalArgumentException if the object is invalid
   */
  public void validate() {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Invalid stage '%s': name must be specified.",
              toString()));
    }
    if (plugin == null) {
      throw new IllegalArgumentException(
          String.format("Invalid stage '%s': plugin must be specified.",
              name));
    }
    if (errorDatasetName != null) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid stage '%s'. Error datasets have been replaced by error collectors. "
                  + "Please connect stage '%s' to an error collector, then connect the error collector "

                  + "to a sink.", name, name));
    }
    plugin.validate();
  }

  // used by UpgradeTool to upgrade a 3.4.x stage to 3.5.x, which may include an update of the plugin artifact
  @Deprecated
  public ETLStage upgradeStage(UpgradeContext upgradeContext) {
    ArtifactSelectorConfig artifactSelectorConfig =
        upgradeContext.getPluginArtifact(plugin.getType(), plugin.getName());
    io.cdap.cdap.etl.proto.v2.ETLPlugin etlPlugin = new io.cdap.cdap.etl.proto.v2.ETLPlugin(
        plugin.getName(), plugin.getType(), plugin.getProperties(), artifactSelectorConfig,
        plugin.getLabel());
    return new io.cdap.cdap.etl.proto.v2.ETLStage(name, etlPlugin);
  }

  /**
   * Updates stage by performing update action logic provided in context. Current relevant update
   * actions for stages are: 1. UPGRADE_ARTIFACT: Upgrades plugin artifact by finding the latest
   * version of plugin to use.
   *
   * @param updateContext Context to use for updating stage.
   * @return new (updated) ETLStage.
   */
  public ETLStage updateStage(ApplicationUpdateContext updateContext) throws Exception {
    for (ApplicationConfigUpdateAction updateAction : updateContext.getUpdateActions()) {
      switch (updateAction) {
        case UPGRADE_ARTIFACT:
          return new io.cdap.cdap.etl.proto.v2.ETLStage(name, upgradePlugin(updateContext), label,
              inputSchema,
              outputSchema, id, information);
        default:
          return this;
      }
    }

    // No update action provided so return stage as is.
    return this;
  }

  public ETLStageValidationResult validateStage(ApplicationValidationContext validationContext) throws Exception {
    ArtifactVersionRange currentVersionRange;
    try {
      currentVersionRange =
          io.cdap.cdap.api.artifact.ArtifactVersionRange.parse(
              plugin.getArtifactConfig().getVersion());
    } catch (Exception e) {
      return new ETLStageValidationResult(
          name, label, id,
          plugin.getName(), plugin.getType(),
          "INVALID_ARTIFACT_VERSION",
          plugin.getArtifactConfig(), null );
    }

    List<ArtifactId> candidates = validationContext.getPluginArtifacts(plugin.getType(), plugin.getName(), null);
    Optional<ArtifactId> newPluginCandidate = candidates.stream()
            .max(Comparator.comparing(artifactId -> artifactId.getVersion()));

    if (!newPluginCandidate.isPresent()) {
      return new ETLStageValidationResult(
          name, label, id,
          plugin.getName(), plugin.getType(),
          "NOT_FOUND",
          plugin.getArtifactConfig(), null );
    }

    ArtifactId matchingArtifact = candidates.stream().filter(artifactId -> {
      if (currentVersionRange.isExactVersion()) {
        return currentVersionRange.getLower().compareTo(artifactId.getVersion()) == 0;
      } else {
        return currentVersionRange.versionIsInRange(artifactId.getVersion());
      }
    }).findFirst().orElse(null);

    ArtifactId newPlugin = newPluginCandidate.get();
    ArtifactSelectorConfig suggestion = new ArtifactSelectorConfig(
        newPlugin.getScope().name(),
        newPlugin.getName(),
        newPlugin.getVersion().getVersion());

    // Current plugin version is exact
    if (currentVersionRange.isExactVersion()) {
      if (currentVersionRange.getLower().compareTo(newPlugin.getVersion()) < 0) {
        // Current version is a fixed version and new version is higher than current.
        if (matchingArtifact == null) {
          return new ETLStageValidationResult(
              name, label, id,
              plugin.getName(), plugin.getType(),
              "VERSION_MISMATCH_EXACT",
              plugin.getArtifactConfig(), suggestion );
        }
        return new ETLStageValidationResult(
              name, label, id,
              plugin.getName(), plugin.getType(),
              "CAN_UPGRADE",
              plugin.getArtifactConfig(), suggestion );
      }
      if (currentVersionRange.getLower().compareTo(newPlugin.getVersion()) == 0) {
        if (plugin.getArtifactConfig().getScope() != newPlugin.getScope().toString()) {
          suggestion = new ArtifactSelectorConfig(
              newPlugin.getScope().toString(),
              plugin.getName(),
              currentVersionRange.getVersionString());

          return new ETLStageValidationResult(
              name, label, id,
              plugin.getName(), plugin.getType(),
              "SCOPE_MISMATCH",
              plugin.getArtifactConfig(), suggestion );
        }
      }
      return null;
    }

    // Current plugin version is version range.
    if (!currentVersionRange.isExactVersion()) {
      if (currentVersionRange.versionIsInRange(newPlugin.getVersion())) {
        // Scope mismatch
        if (plugin.getArtifactConfig().getScope() != newPlugin.getScope().toString()) {
          suggestion = new ArtifactSelectorConfig(
              newPlugin.getScope().toString(),
              plugin.getName(),
              currentVersionRange.getVersionString());

          return new ETLStageValidationResult(
              name, label, id,
              plugin.getName(), plugin.getType(),
              "SCOPE_MISMATCH",
              plugin.getArtifactConfig(), suggestion );
        }

        return null;
      }
      // Current lower version is higher than newer latest version.
      if (currentVersionRange.getLower().compareTo(newPlugin.getVersion()) > 0) {

        ArtifactVersionRange newVersionRange =
            new ArtifactVersionRange(newPlugin.getVersion(),
                true,
                currentVersionRange.getUpper(), currentVersionRange.isUpperInclusive());

        suggestion = new ArtifactSelectorConfig(
            newPlugin.getScope().name(),
            newPlugin.getName(),
            newVersionRange.getVersionString());

        return new ETLStageValidationResult(
            name, label, id,
            plugin.getName(), plugin.getType(),
            "VERSION_MISMATCH_DOWNGRADE_RANGE",
            plugin.getArtifactConfig(), suggestion );
      }
      // Increase the upper bound to latest available version.
      ArtifactVersionRange newVersionRange =
          new ArtifactVersionRange(currentVersionRange.getLower(),
              currentVersionRange.isLowerInclusive(),
              newPlugin.getVersion(), true);

      suggestion = new ArtifactSelectorConfig(
          newPlugin.getScope().name(),
          newPlugin.getName(),
          newVersionRange.getVersionString());

      return new ETLStageValidationResult(
          name, label, id,
          plugin.getName(), plugin.getType(),
          "VERSION_MISMATCH_UPGRADE_RANGE",
          plugin.getArtifactConfig(), suggestion );
    }
    return null;
  }

  /**
   * Upgrade plugin used in the stage. 1. If plugin is using fixed version and a new plugin artifact
   * is found with higher version in SYSTEM scope, use the new plugin. 2. If plugin is using a
   * plugin range and a new plugin artifact is found with higher version in SYSTEM scope, move the
   * upper bound of the range to include the new plugin artifact. Also change plugin scope. If new
   * plugin is in range, do not change range. (Note: It would not change range even though new
   * plugin is in different scope).
   *
   * @param updateContext To use helper functions like getPluginArtifacts.
   * @return Updated plugin object to be used for the udated stage. Returned null if no changes to
   *     current plugin.
   */
  private ETLPlugin upgradePlugin(ApplicationUpdateContext updateContext) throws Exception {
    // Find the plugin with max version from available candidates.
    Optional<ArtifactId> newPluginCandidate =
        updateContext.getPluginArtifacts(plugin.getType(), plugin.getName(), null).stream()
            .max(Comparator.comparing(artifactId -> artifactId.getVersion()));
    if (!newPluginCandidate.isPresent()) {
      // This should not happen as there should be at least one plugin candidate same as current.
      // TODO: Consider throwing exception here.
      return plugin;
    }

    ArtifactId newPlugin = newPluginCandidate.get();
    String newVersion = getUpgradedVersionString(newPlugin);
    // If getUpgradedVersionString returns null, candidate plugin is not valid for upgrade.
    if (newVersion == null) {
      return plugin;
    }

    ArtifactSelectorConfig newArtifactSelectorConfig =
        new ArtifactSelectorConfig(newPlugin.getScope().name(), newPlugin.getName(),
            newVersion);
    io.cdap.cdap.etl.proto.v2.ETLPlugin upgradedEtlPlugin =
        new io.cdap.cdap.etl.proto.v2.ETLPlugin(plugin.getName(), plugin.getType(),
            plugin.getProperties(),
            newArtifactSelectorConfig, plugin.getLabel());
    return upgradedEtlPlugin;
  }

  /**
   * Returns new valid version string for plugin upgrade if any changes are required. Returns null
   * if no change to current plugin version. Artifact selector config only stores plugin version as
   * string, it can be either fixed version or range. Hence, if the plugin version is fixed, replace
   * the fixed version with newer fixed version. If it is a range, move the upper bound of the range
   * to the newest version.
   *
   * @param newPlugin New candidate plugin for updating plugin artifact.
   * @return version string to be used for new plugin. Might be fixed version/version range string
   *     depending on current use.
   */
  @Nullable
  private String getUpgradedVersionString(ArtifactId newPlugin) {
    ArtifactVersionRange currentVersionRange;
    try {
      currentVersionRange =
          io.cdap.cdap.api.artifact.ArtifactVersionRange.parse(
              plugin.getArtifactConfig().getVersion());
    } catch (Exception e) {
      LOG.warn("Issue in parsing version string for plugin {}, ignoring stage {} for upgrade.",
          plugin, name, e);
      return null;
    }

    if (currentVersionRange.isExactVersion()) {
      if (currentVersionRange.getLower().compareTo(newPlugin.getVersion()) < 0) {
        // Current version is a fixed version and new version is higher than current.
        return newPlugin.getVersion().getVersion();
      }
      return null;
    }

    if (!currentVersionRange.isExactVersion()) {
      // Current plugin version is version range.
      if (currentVersionRange.versionIsInRange(newPlugin.getVersion())) {
        // Do nothing and return as is. Note that plugin scope will not change.
        // TODO: Figure out how to change plugin scope if a newer plugin is found but in different scope.
        return null;
      }
      // Current lower version is higher than newer latest version. This should not happen.
      if (currentVersionRange.getLower().compareTo(newPlugin.getVersion()) > 0) {
        LOG.warn("Error in updating stage {}. Invalid new plugin artifact {} upgrading plugin {}.",
            name, newPlugin, plugin);
        return null;
      }
      // Increase the upper bound to latest available version.
      ArtifactVersionRange newVersionRange =
          new ArtifactVersionRange(currentVersionRange.getLower(),
              currentVersionRange.isLowerInclusive(),
              newPlugin.getVersion(), true);
      return newVersionRange.getVersionString();
    }
    return null;
  }

  @Override
  public String toString() {
    return "ETLStage{"
        + "name='" + name + '\''
        + ", plugin=" + plugin
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ETLStage that = (ETLStage) o;

    return Objects.equals(name, that.name)
        && Objects.equals(plugin, that.plugin);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, plugin);
  }
}
