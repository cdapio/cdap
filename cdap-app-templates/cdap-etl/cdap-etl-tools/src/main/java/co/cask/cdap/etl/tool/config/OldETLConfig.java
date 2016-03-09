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

package co.cask.cdap.etl.tool.config;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.common.ArtifactSelectorConfig;
import co.cask.cdap.etl.common.ETLConfig;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.proto.artifact.ArtifactSummary;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Common ETL Config.
 */
public abstract class OldETLConfig extends Config {
  private final OldETLStage source;
  private final List<OldETLStage> sinks;
  private final List<OldETLStage> transforms;
  private final Resources resources;

  public OldETLConfig(OldETLStage source, List<OldETLStage> sinks, List<OldETLStage> transforms, Resources resources) {
    this.source = source;
    this.sinks = sinks;
    this.transforms = transforms;
    this.resources = resources;
  }

  protected void buildNewConfig(ETLConfig.Builder builder, PluginArtifactFinder pluginArtifactFinder) {
    int pluginNum = 1;

    String sourceName = source.getName() + "." + pluginNum;
    Plugin sourcePlugin = new Plugin(
      source.getName(),
      source.getProperties(),
      getArtifact(pluginArtifactFinder.getSourcePluginArtifact(source.getName()))
    );
    builder.setSource(new ETLStage(sourceName, sourcePlugin, source.getErrorDatasetName()));
    String prevStageName = sourceName;
    if (transforms != null) {
      for (OldETLStage transform : transforms) {
        pluginNum++;
        String transformName = transform.getName() + "." + pluginNum;
        Plugin transformPlugin = new Plugin(
          transform.getName(),
          transform.getProperties(),
          getArtifact(pluginArtifactFinder.getTransformPluginArtifact(transform.getName()))
        );
        builder.addTransform(new ETLStage(transformName, transformPlugin, transform.getErrorDatasetName()));
        builder.addConnection(prevStageName, transformName);
        prevStageName = transformName;
      }
    }

    for (OldETLStage sink : sinks) {
      pluginNum++;
      String sinkName = sink.getName() + "." + pluginNum;
      Plugin sinkPlugin = new Plugin(
        sink.getName(),
        sink.getProperties(),
        getArtifact(pluginArtifactFinder.getSinkPluginArtifact(sink.getName()))
      );
      builder.addSink(new ETLStage(sinkName, sinkPlugin, sink.getErrorDatasetName()));
      builder.addConnection(prevStageName, sinkName);
    }
    builder.setResources(resources);
  }

  @Nullable
  private ArtifactSelectorConfig getArtifact(@Nullable ArtifactSummary artifactSummary) {
    if (artifactSummary == null) {
      return null;
    }
    return new ArtifactSelectorConfig(
      artifactSummary.getScope().name(), artifactSummary.getName(), artifactSummary.getVersion());
  }
}
