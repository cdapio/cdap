/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.spi.metadata;

import io.cdap.cdap.api.annotation.Beta;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Lineage information for a program run.
 */
@Beta
public class LineageInfo {

  private final Set<Asset> sources;
  private final Set<Asset> targets;
  private final Map<Asset, Set<Asset>> sourceToTargets;
  private final Map<Asset, Set<Asset>> targetToSources;
  private final Long startTimeMs;
  private final Long endTimeMs;
  private final String lineageId;

  private LineageInfo(Set<Asset> sources, Set<Asset> targets,
      Map<Asset, Set<Asset>> sourceToTargets,
      Map<Asset, Set<Asset>> targetToSources, @Nullable Long startTimeMs,
      @Nullable Long endTimeMs, String lineageId) {
    this.sources = Collections.unmodifiableSet(new HashSet<>(sources));
    this.targets = Collections.unmodifiableSet(new HashSet<>(targets));
    this.sourceToTargets = Collections.unmodifiableMap(new HashMap<>(sourceToTargets));
    this.targetToSources = Collections.unmodifiableMap(new HashMap<>(targetToSources));
    this.startTimeMs = startTimeMs;
    this.endTimeMs = endTimeMs;
    this.lineageId = lineageId;
  }

  /**
   * Returns the set of all source assets for a program run.
   */
  public Set<Asset> getSources() {
    return sources;
  }

  /**
   * Returns the set of all target assets for a program run.
   */
  public Set<Asset> getTargets() {
    return targets;
  }

  /**
   * Returns the map of all source assets to their corresponding set of target assets.
   */
  public Map<Asset, Set<Asset>> getSourceToTargets() {
    return sourceToTargets;
  }

  /**
   * Returns the map of all target assets to their corresponding set of source assets.
   */
  public Map<Asset, Set<Asset>> getTargetToSources() {
    return targetToSources;
  }

  /**
   * Returns the time at which lineage was started to get captured.
   */
  @Nullable
  public Long getStartTimeMs() {
    return startTimeMs;
  }

  /**
   * Returns the time at which lineage capture ended.
   */
  @Nullable
  public Long getEndTimeMs() {
    return endTimeMs;
  }

  /**
   * Returns the lineage ID.
   */
  public String getLineageId() {
    return lineageId;
  }

  @Override
  public String toString() {
    return "LineageInfo{"
        + "sources=" + sources
        + ", targets=" + targets
        + ", sourceToTargets=" + sourceToTargets
        + ", targetToSources=" + targetToSources
        + ", startTimeMs=" + startTimeMs
        + ", endTimeMs=" + endTimeMs
        + ", lineageId=" + lineageId
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
    LineageInfo that = (LineageInfo) o;
    return sources.equals(that.sources)
        && targets.equals(that.targets)
        && sourceToTargets.equals(that.sourceToTargets)
        && targetToSources.equals(that.targetToSources)
        && Objects.equals(startTimeMs, that.startTimeMs)
        && Objects.equals(endTimeMs, that.endTimeMs)
        && Objects.equals(lineageId, that.lineageId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sources, targets, sourceToTargets, targetToSources, startTimeMs, endTimeMs,
        lineageId);
  }

  public static LineageInfo.Builder builder() {
    return new Builder();
  }

  /**
   * A builder to create {@link LineageInfo} instance.
   */
  public static final class Builder {

    private Set<Asset> sources;
    private Set<Asset> targets;
    private final Map<Asset, Set<Asset>> sourceToTargets;
    private final Map<Asset, Set<Asset>> targetToSources;
    private Long startTimeMs;
    private Long endTimeMs;
    private String lineageId;

    private Builder() {
      this.sourceToTargets = new HashMap<>();
      this.targetToSources = new HashMap<>();
    }

    /**
     * Sets the set of all sources in a program run.
     */
    public Builder setSources(Set<Asset> sources) {
      this.sources = Collections.unmodifiableSet(new HashSet<>(sources));
      return this;
    }

    /**
     * Appends a source asset to an existing set of all sources of a program run.
     */
    public Builder addSource(Asset source) {
      this.sources.add(source);
      return this;
    }

    /**
     * Appends multiple source assets to an existing set of all sources of a program run.
     */
    public Builder addSources(Set<Asset> sources) {
      this.sources.addAll(sources);
      return this;
    }

    /**
     * Set the set of all targets/destinations in a program run.
     */
    public Builder setTargets(Set<Asset> targets) {
      this.targets = Collections.unmodifiableSet(new HashSet<>(targets));
      return this;
    }

    /**
     * Appends a target asset to an existing set of all targets of a program run.
     */
    public Builder addTarget(Asset target) {
      this.targets.add(target);
      return this;
    }

    /**
     * Appends multiple target assets to an existing set of all targets of a program run.
     */
    public Builder addTargets(Set<Asset> targets) {
      this.targets.addAll(targets);
      return this;
    }

    /**
     * Sets the start time of the program run.
     */
    public Builder setStartTimeMs(@Nullable Long startTimeMs) {
      this.startTimeMs = startTimeMs;
      return this;
    }

    /**
     * Sets the end time of the program run.
     */
    public Builder setEndTimeMs(@Nullable Long endTimeMs) {
      this.endTimeMs = endTimeMs;
      return this;
    }

    /**
     * Sets the id of LineageInfo.
     */
    public Builder setLineageId(String lineageId) {
      this.lineageId = lineageId;
      return this;
    }

    /**
     * Set and replace the source to target assets map.
     *
     * @param sourceToTargets map of source to target assets to add.
     * @return this builder
     */
    public Builder setSourceToTargets(Map<Asset, Set<Asset>> sourceToTargets) {
      this.sourceToTargets.clear();
      sourceToTargets.keySet()
          .forEach(key -> this.sourceToTargets.put(key, Collections.unmodifiableSet(
              new HashSet<>(sourceToTargets.get(key)))));
      return this;
    }

    /**
     * Adds multiple source to target assets map.
     *
     * @param sourceToTargets map of source to target assets to add.
     * @return this builder
     */
    public Builder addAllSourceToTargets(Map<Asset, Set<Asset>> sourceToTargets) {
      this.sourceToTargets.putAll(sourceToTargets);
      return this;
    }

    /**
     * Adds a source to target assets map.
     *
     * @param source the source asset
     * @param targets the set of target assets for the source asset
     * @return this builder
     */
    public Builder addSourceToTargets(Asset source, Set<Asset> targets) {
      this.sourceToTargets.put(source, targets);
      return this;
    }

    /**
     * Set and replace the target to source assets map.
     *
     * @param targetToSources map of target to source assets to add.
     * @return this builder
     */
    public Builder setTargetToSources(Map<Asset, Set<Asset>> targetToSources) {
      this.targetToSources.clear();
      targetToSources.keySet()
          .forEach(key -> this.targetToSources.put(key, Collections.unmodifiableSet(
              new HashSet<>(targetToSources.get(key)))));
      return this;
    }

    /**
     * Adds multiple target to source assets map.
     *
     * @param targetToSources map of target to source assets to add.
     * @return this builder
     */
    public Builder addAllTargetToSources(Map<Asset, Set<Asset>> targetToSources) {
      this.targetToSources.putAll(targetToSources);
      return this;
    }

    /**
     * Adds a target to source assets map.
     *
     * @param target the target asset
     * @param sources the set of source assets for the target asset
     * @return this builder
     */
    public Builder addTargetToSources(Asset target, Set<Asset> sources) {
      this.targetToSources.put(target, sources);
      return this;
    }

    /**
     * Creates a new instance of {@link LineageInfo}.
     */
    public LineageInfo build() {
      return new LineageInfo(sources, targets, sourceToTargets, targetToSources, startTimeMs,
          endTimeMs, lineageId);
    }
  }
}
