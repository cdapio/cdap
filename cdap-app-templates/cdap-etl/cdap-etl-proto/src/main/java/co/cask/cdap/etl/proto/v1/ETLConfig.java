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

package co.cask.cdap.etl.proto.v1;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.Connection;
import co.cask.cdap.etl.proto.UpgradeContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Common ETL Config.
 */
public class ETLConfig extends Config {
  private Boolean stageLoggingEnabled;
  private final ETLStage source;
  private final List<ETLStage> sinks;
  private final List<ETLStage> transforms;
  private final List<Connection> connections;
  private final Resources resources;

  /**
   * @deprecated use builders from subclasses instead.
   */
  @Deprecated
  public ETLConfig(ETLStage source, List<ETLStage> sinks, List<ETLStage> transforms,
                   List<Connection> connections, Resources resources) {
    this(source, sinks, transforms, connections, resources, true);
  }

  protected ETLConfig(ETLStage source, List<ETLStage> sinks, List<ETLStage> transforms,
                      List<Connection> connections, Resources resources, boolean stageLoggingEnabled) {
    this.source = source;
    this.sinks = sinks;
    this.transforms = transforms;
    this.connections = getValidConnections(connections);
    this.resources = resources;
    this.stageLoggingEnabled = stageLoggingEnabled;
  }

  private List<Connection> getValidConnections(List<Connection> connections) {
    // TODO : this can be removed once UI changes are made and we don't have to support the old format
    if (source.getPlugin() == null) {
      // if its old format, we just return an empty list.
      return new ArrayList<>();
    }

    if (connections == null) {
      connections = new ArrayList<>();
    }
    if (connections.isEmpty()) {
      // if connections are empty, we create a connections list,
      // which is a linear pipeline, source -> transforms -> sinks
      String toSink = source.getName();
      if (transforms != null && !transforms.isEmpty()) {
        connections.add(new Connection(source.getName(), transforms.get(0).getName()));
        for (int i = 0; i < transforms.size() - 1; i++) {
          connections.add(new Connection(transforms.get(i).getName(), transforms.get(i + 1).getName()));
        }
        toSink = transforms.get(transforms.size() - 1).getName();
      }
      for (ETLStage stage : sinks) {
        connections.add(new Connection(toSink, stage.getName()));
      }
    }
    return connections;
  }

  public ETLConfig(ETLStage source, ETLStage sink, List<ETLStage> transforms,
                   List<Connection> connections, Resources resources) {
    this(source, ImmutableList.of(sink), transforms, connections, resources);
  }

  public ETLConfig getCompatibleConfig() {
    int pluginNum = 1;
    ETLStage sourceStage = source.getCompatibleStage("source." + source.getName() +  "." + pluginNum);
    List<ETLStage> transformStages = new ArrayList<>();
    if (transforms != null) {
      for (ETLStage transform : transforms) {
        pluginNum++;
        transformStages.add(transform.getCompatibleStage("transform." + transform.getName() + "." + pluginNum));
      }
    }
    List<ETLStage> sinkStages = new ArrayList<>();
    for (ETLStage sink : sinks) {
      pluginNum++;
      sinkStages.add(sink.getCompatibleStage("sink." + sink.getName() + "." + pluginNum));
    }
    List<Connection> connectionList = connections == null ? new ArrayList<Connection>() : connections;
    return new ETLConfig(sourceStage, sinkStages, transformStages, connectionList, resources);
  }

  public ETLStage getSource() {
    return source;
  }

  public List<ETLStage> getSinks() {
    return sinks;
  }

  public List<ETLStage> getTransforms() {
    return transforms != null ? transforms : Lists.<ETLStage>newArrayList();
  }

  public List<Connection> getConnections() {
    return connections;
  }

  public Resources getResources() {
    return resources;
  }

  public Boolean isStageLoggingEnabled() {
    return stageLoggingEnabled == null ? true : stageLoggingEnabled;
  }

  protected <T extends co.cask.cdap.etl.proto.v2.ETLConfig.Builder> T upgradeBase(T builder,
                                                                                  UpgradeContext upgradeContext,
                                                                                  String sourceType,
                                                                                  String sinkType) {
    builder.setResources(resources).addConnections(connections);

    if (!stageLoggingEnabled) {
      builder.disableStageLogging();
    }

    builder.addStage(getSource().upgradeStage(sourceType, upgradeContext));
    for (ETLStage transformStage : getTransforms()) {
      builder.addStage(transformStage.upgradeStage(Transform.PLUGIN_TYPE, upgradeContext));
    }
    for (ETLStage sinkStage : getSinks()) {
      builder.addStage(sinkStage.upgradeStage(sinkType, upgradeContext));
    }
    return builder;
  }

  @Override
  public String toString() {
    return "ETLConfig{" +
      "stageLoggingEnabled=" + stageLoggingEnabled +
      ", source=" + source +
      ", sinks=" + sinks +
      ", transforms=" + transforms +
      ", connections=" + connections +
      ", resources=" + resources +
      "} " + super.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ETLConfig that = (ETLConfig) o;

    return Objects.equals(source, that.source) &&
      Objects.equals(sinks, that.sinks) &&
      Objects.equals(transforms, that.transforms) &&
      Objects.equals(connections, that.connections) &&
      Objects.equals(resources, that.resources) &&
      isStageLoggingEnabled() == that.isStageLoggingEnabled();
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, sinks, transforms, connections, resources, isStageLoggingEnabled());
  }

  /**
   * Builder for creating configs.
   */
  @SuppressWarnings("unchecked")
  public abstract static class Builder<T extends Builder> {
    protected ETLStage source;
    protected List<ETLStage> sinks;
    protected List<ETLStage> transforms;
    protected List<Connection> connections;
    protected Resources resources;
    protected Boolean stageLoggingEnabled;

    protected Builder() {
      this.sinks = new ArrayList<>();
      this.transforms = new ArrayList<>();
      this.connections = new ArrayList<>();
      this.resources = new Resources();
      this.stageLoggingEnabled = true;
    }

    public T setSource(ETLStage source) {
      this.source = source;
      return (T) this;
    }

    public T addTransform(ETLStage transform) {
      this.transforms.add(transform);
      return (T) this;
    }

    public T addTransforms(Collection<ETLStage> transforms) {
      this.transforms.addAll(transforms);
      return (T) this;
    }

    public T addSink(ETLStage sink) {
      this.sinks.add(sink);
      return (T) this;
    }

    public T addSinks(Collection<ETLStage> sinks) {
      this.sinks.addAll(sinks);
      return (T) this;
    }

    public T addConnection(String from, String to) {
      this.connections.add(new Connection(from, to));
      return (T) this;
    }

    public T addConnection(Connection connection) {
      this.connections.add(connection);
      return (T) this;
    }

    public T addConnections(Collection<Connection> connections) {
      this.connections.addAll(connections);
      return (T) this;
    }

    public T setResources(Resources resources) {
      this.resources = resources;
      return (T) this;
    }

    public T disableStageLogging() {
      this.stageLoggingEnabled = false;
      return (T) this;
    }
  }
}
